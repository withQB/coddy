package storage

import (
	"context"

	"github.com/withqb/coddy/apis/mediaapi/types"
	"github.com/withqb/xtools/spec"
)

type Database interface {
	MediaRepository
	Thumbnails
}

type MediaRepository interface {
	StoreMediaMetadata(ctx context.Context, mediaMetadata *types.MediaMetadata) error
	GetMediaMetadata(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName) (*types.MediaMetadata, error)
	GetMediaMetadataByHash(ctx context.Context, mediaHash types.Base64Hash, mediaOrigin spec.ServerName) (*types.MediaMetadata, error)
}

type Thumbnails interface {
	StoreThumbnail(ctx context.Context, thumbnailMetadata *types.ThumbnailMetadata) error
	GetThumbnail(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName, width, height int, resizeMethod string) (*types.ThumbnailMetadata, error)
	GetThumbnails(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName) ([]*types.ThumbnailMetadata, error)
}
