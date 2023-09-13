package sqlite3

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"strings"

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/servers/dataframe/storage/tables"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools/spec"
)

const userFrameKeysSchema = `
CREATE TABLE IF NOT EXISTS dataframe_user_frame_keys (     
    user_nid    INTEGER NOT NULL,
    frame_nid    INTEGER NOT NULL,
    pseudo_id_key TEXT NULL, -- may be null for users not local to the server
    pseudo_id_pub_key TEXT NOT NULL,
    CONSTRAINT dataframe_user_frame_keys_pk PRIMARY KEY (user_nid, frame_nid)
);
`

const insertUserFrameKeySQL = `
	INSERT INTO dataframe_user_frame_keys (user_nid, frame_nid, pseudo_id_key, pseudo_id_pub_key) VALUES ($1, $2, $3, $4)
	ON CONFLICT DO UPDATE SET pseudo_id_key = dataframe_user_frame_keys.pseudo_id_key
	RETURNING (pseudo_id_key)
`

const insertUserFramePublicKeySQL = `
	INSERT INTO dataframe_user_frame_keys (user_nid, frame_nid, pseudo_id_pub_key) VALUES ($1, $2, $3)
	ON CONFLICT DO UPDATE SET pseudo_id_pub_key = $3
	RETURNING (pseudo_id_pub_key)
`

const selectUserFrameKeySQL = `SELECT pseudo_id_key FROM dataframe_user_frame_keys WHERE user_nid = $1 AND frame_nid = $2`

const selectUserFramePublicKeySQL = `SELECT pseudo_id_pub_key FROM dataframe_user_frame_keys WHERE user_nid = $1 AND frame_nid = $2`

const selectUserNIDsSQL = `SELECT user_nid, frame_nid, pseudo_id_pub_key FROM dataframe_user_frame_keys WHERE frame_nid IN ($1) AND pseudo_id_pub_key IN ($2)`

const selectAllUserFramePublicKeyForUserSQL = `SELECT frame_nid, pseudo_id_pub_key FROM dataframe_user_frame_keys WHERE user_nid = $1`

type userFrameKeysStatements struct {
	db                                 *sql.DB
	insertUserFramePrivateKeyStmt       *sql.Stmt
	insertUserFramePublicKeyStmt        *sql.Stmt
	selectUserFrameKeyStmt              *sql.Stmt
	selectUserFramePublicKeyStmt        *sql.Stmt
	selectAllUserFramePublicKeysForUser *sql.Stmt
	//selectUserNIDsStmt           *sql.Stmt //prepared at runtime
}

func CreateUserFrameKeysTable(db *sql.DB) error {
	_, err := db.Exec(userFrameKeysSchema)
	return err
}

func PrepareUserFrameKeysTable(db *sql.DB) (tables.UserFrameKeys, error) {
	s := &userFrameKeysStatements{db: db}
	return s, sqlutil.StatementList{
		{&s.insertUserFramePrivateKeyStmt, insertUserFrameKeySQL},
		{&s.insertUserFramePublicKeyStmt, insertUserFramePublicKeySQL},
		{&s.selectUserFrameKeyStmt, selectUserFrameKeySQL},
		{&s.selectUserFramePublicKeyStmt, selectUserFramePublicKeySQL},
		{&s.selectAllUserFramePublicKeysForUser, selectAllUserFramePublicKeyForUserSQL},
		//{&s.selectUserNIDsStmt, selectUserNIDsSQL}, //prepared at runtime
	}.Prepare(db)
}

func (s *userFrameKeysStatements) InsertUserFramePrivatePublicKey(ctx context.Context, txn *sql.Tx, userNID types.EventStateKeyNID, frameNID types.FrameNID, key ed25519.PrivateKey) (result ed25519.PrivateKey, err error) {
	stmt := sqlutil.TxStmtContext(ctx, txn, s.insertUserFramePrivateKeyStmt)
	err = stmt.QueryRowContext(ctx, userNID, frameNID, key, key.Public()).Scan(&result)
	return result, err
}

func (s *userFrameKeysStatements) InsertUserFramePublicKey(ctx context.Context, txn *sql.Tx, userNID types.EventStateKeyNID, frameNID types.FrameNID, key ed25519.PublicKey) (result ed25519.PublicKey, err error) {
	stmt := sqlutil.TxStmtContext(ctx, txn, s.insertUserFramePublicKeyStmt)
	err = stmt.QueryRowContext(ctx, userNID, frameNID, key).Scan(&result)
	return result, err
}

func (s *userFrameKeysStatements) SelectUserFramePrivateKey(
	ctx context.Context,
	txn *sql.Tx,
	userNID types.EventStateKeyNID,
	frameNID types.FrameNID,
) (ed25519.PrivateKey, error) {
	stmt := sqlutil.TxStmtContext(ctx, txn, s.selectUserFrameKeyStmt)
	var result ed25519.PrivateKey
	err := stmt.QueryRowContext(ctx, userNID, frameNID).Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return result, err
}

func (s *userFrameKeysStatements) SelectUserFramePublicKey(
	ctx context.Context,
	txn *sql.Tx,
	userNID types.EventStateKeyNID,
	frameNID types.FrameNID,
) (ed25519.PublicKey, error) {
	stmt := sqlutil.TxStmtContext(ctx, txn, s.selectUserFramePublicKeyStmt)
	var result ed25519.PublicKey
	err := stmt.QueryRowContext(ctx, userNID, frameNID).Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return result, err
}

func (s *userFrameKeysStatements) BulkSelectUserNIDs(ctx context.Context, txn *sql.Tx, senderKeys map[types.FrameNID][]ed25519.PublicKey) (map[string]types.UserFrameKeyPair, error) {

	frameNIDs := make([]any, 0, len(senderKeys))
	var senders []any
	for frameNID := range senderKeys {
		frameNIDs = append(frameNIDs, frameNID)

		for _, key := range senderKeys[frameNID] {
			senders = append(senders, []byte(key))
		}
	}

	selectSQL := strings.Replace(selectUserNIDsSQL, "($2)", sqlutil.QueryVariadicOffset(len(senders), len(senderKeys)), 1)
	selectSQL = strings.Replace(selectSQL, "($1)", sqlutil.QueryVariadic(len(senderKeys)), 1) // replace $1 with the frameNIDs

	selectStmt, err := s.db.Prepare(selectSQL)
	if err != nil {
		return nil, err
	}

	params := append(frameNIDs, senders...)

	stmt := sqlutil.TxStmt(txn, selectStmt)
	defer internal.CloseAndLogIfError(ctx, stmt, "failed to close statement")

	rows, err := stmt.QueryContext(ctx, params...)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")

	result := make(map[string]types.UserFrameKeyPair, len(params))
	var publicKey []byte
	userFrameKeyPair := types.UserFrameKeyPair{}
	for rows.Next() {
		if err = rows.Scan(&userFrameKeyPair.EventStateKeyNID, &userFrameKeyPair.FrameNID, &publicKey); err != nil {
			return nil, err
		}
		result[spec.Base64Bytes(publicKey).Encode()] = userFrameKeyPair
	}
	return result, rows.Err()
}

func (s *userFrameKeysStatements) SelectAllPublicKeysForUser(ctx context.Context, txn *sql.Tx, userNID types.EventStateKeyNID) (map[types.FrameNID]ed25519.PublicKey, error) {
	stmt := sqlutil.TxStmtContext(ctx, txn, s.selectAllUserFramePublicKeysForUser)

	rows, err := stmt.QueryContext(ctx, userNID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}

	resultMap := make(map[types.FrameNID]ed25519.PublicKey)

	var frameNID types.FrameNID
	var pubkey ed25519.PublicKey
	for rows.Next() {
		if err = rows.Scan(&frameNID, &pubkey); err != nil {
			return nil, err
		}
		resultMap[frameNID] = pubkey
	}
	return resultMap, err
}
