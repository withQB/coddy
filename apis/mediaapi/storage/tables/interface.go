package tables

import (
	"context"
	"database/sql"

	"github.com/withqb/coddy/apis/mediaapi/types"
	"github.com/withqb/xtools/spec"
)

type Thumbnails interface {
	InsertThumbnail(ctx context.Context, txn *sql.Tx, thumbnailMetadata *types.ThumbnailMetadata) error
	SelectThumbnail(
		ctx context.Context, txn *sql.Tx,
		mediaID types.MediaID, mediaOrigin spec.ServerName,
		width, height int,
		resizeMethod string,
	) (*types.ThumbnailMetadata, error)
	SelectThumbnails(
		ctx context.Context, txn *sql.Tx, mediaID types.MediaID,
		mediaOrigin spec.ServerName,
	) ([]*types.ThumbnailMetadata, error)
}

type MediaRepository interface {
	InsertMedia(ctx context.Context, txn *sql.Tx, mediaMetadata *types.MediaMetadata) error
	SelectMedia(ctx context.Context, txn *sql.Tx, mediaID types.MediaID, mediaOrigin spec.ServerName) (*types.MediaMetadata, error)
	SelectMediaByHash(
		ctx context.Context, txn *sql.Tx,
		mediaHash types.Base64Hash, mediaOrigin spec.ServerName,
	) (*types.MediaMetadata, error)
}
