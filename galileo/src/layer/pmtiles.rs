//! PMTiles support for Galileo

use std::io::Read;

use bytes::Bytes;
use flate2::read::GzDecoder;
use galileo_mvt::MvtTile;
use log::error;
use pmtiles::TileCoord;

use crate::decoded_image::DecodedImage;
use crate::error::GalileoError;
use crate::layer::raster_tile_layer::RasterTileLoader;
use crate::layer::vector_tile_layer::tile_provider::loader::{TileLoadError, VectorTileLoader};
use crate::platform::PlatformService;
use crate::tile_schema::TileIndex;

/// Tile loader for PMTiles format using HTTP backend
pub struct PmtilesTileLoader {
    reader: pmtiles::AsyncPmTilesReader<pmtiles::HttpBackend>,
}

impl PmtilesTileLoader {
    /// Creates a new PMTiles tile loader with the given reader
    pub fn new(reader: pmtiles::AsyncPmTilesReader<pmtiles::HttpBackend>) -> Self {
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
impl RasterTileLoader for PmtilesTileLoader {
    async fn load(&self, index: TileIndex) -> Result<DecodedImage, GalileoError> {
        let bytes = self.get_tile(index).await?;
        crate::platform::instance().decode_image(bytes).await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl VectorTileLoader for PmtilesTileLoader {
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
