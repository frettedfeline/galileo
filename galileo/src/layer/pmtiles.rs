//! PMTiles support for Galileo

use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use flate2::read::GzDecoder;
use galileo_mvt::MvtTile;
use log::error;
use pmtiles::{DirCacheResult, Directory, DirectoryCache, TileCoord, TileId};

use crate::decoded_image::DecodedImage;
use crate::error::GalileoError;
use crate::layer::raster_tile_layer::RasterTileLoader;
use crate::layer::vector_tile_layer::tile_provider::loader::{TileLoadError, VectorTileLoader};
use crate::platform::PlatformService;
use crate::tile_schema::TileIndex;

/// A simple HashMap-based implementation of the `pmtiles::DirectoryCache` trait.
#[derive(Default, Clone)]
pub struct PmtilesDirCache {
    cache: Arc<RwLock<HashMap<usize, Directory>>>,
}

impl PmtilesDirCache {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DirectoryCache for PmtilesDirCache {
    async fn get_dir_entry(&self, offset: usize, tile_id: TileId) -> DirCacheResult {
        #[allow(clippy::unwrap_used)]
        if let Some(dir) = self.cache.read().unwrap().get(&offset) {
            if let Some(entry) = dir.find_tile_id(tile_id) {
                return DirCacheResult::Found(entry.clone());
            } else {
                return DirCacheResult::NotFound;
            }
        }
        DirCacheResult::NotCached
    }

    async fn insert_dir(&self, offset: usize, directory: Directory) {
        #[allow(clippy::unwrap_used)]
        self.cache.write().unwrap().insert(offset, directory);
    }
}

/// Tile loader for PMTiles format using an async backend (e.g., HTTP)
pub struct PmtilesTileLoader<B = pmtiles::HttpBackend, C = pmtiles::NoCache> {
    reader: pmtiles::AsyncPmTilesReader<B, C>,
}

impl<B, C> PmtilesTileLoader<B, C>
where
    B: pmtiles::AsyncBackend + Send + Sync,
    C: DirectoryCache + Send + Sync,
{
    /// Creates a new PMTiles tile loader with the given reader
    pub fn new(reader: pmtiles::AsyncPmTilesReader<B, C>) -> Self {
        Self { reader }
    }

    async fn get_tile(&self, index: TileIndex) -> Result<Bytes, GalileoError> {
        let coord = TileCoord::new(index.z as u8, index.x as u32, index.y as u32)
            .ok_or(GalileoError::NotFound)?;

        self.reader
            .get_tile(coord)
            .await
            .map_err(|_| GalileoError::NotFound)?
            .ok_or(GalileoError::NotFound)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<B, C> RasterTileLoader for PmtilesTileLoader<B, C>
where
    B: pmtiles::AsyncBackend + Send + Sync,
    C: DirectoryCache + Send + Sync + maybe_sync::MaybeSend + maybe_sync::MaybeSync,
{
    async fn load(&self, index: TileIndex) -> Result<DecodedImage, GalileoError> {
        let bytes = self.get_tile(index).await?;
        crate::platform::instance().decode_image(bytes).await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<B, C> VectorTileLoader for PmtilesTileLoader<B, C>
where
    B: pmtiles::AsyncBackend + Send + Sync,
    C: DirectoryCache + Send + Sync + maybe_sync::MaybeSend + maybe_sync::MaybeSync,
{
    async fn load(&self, index: TileIndex) -> Result<MvtTile, TileLoadError> {
        let bytes = self
            .get_tile(index)
            .await
            .map_err(|_| TileLoadError::Network)?;

        // Check if this is GZIP compressed data
        let decompressed_bytes = if bytes.len() > 2 && bytes[0..2] == [0x1F, 0x8B] {
            // GZIP compressed data - decompress it
            let mut decoder = GzDecoder::new(&bytes[..]);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).map_err(|e| {
                error!("PMTiles: GZIP decompression error: {:?}", e);
                TileLoadError::Decoding
            })?;
            Bytes::from(decompressed)
        } else {
            // Not compressed, use as-is
            bytes
        };

        MvtTile::decode(decompressed_bytes, false).map_err(|e| {
            error!("PMTiles: Vector tile decoding error: {:?}", e);
            TileLoadError::Decoding
        })
    }
}
