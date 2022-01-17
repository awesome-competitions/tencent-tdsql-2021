package consts

const (
	LF                  = byte('\n')
	COMMA               = byte(',')
	K                   = 1024
	M                   = 1024 * K
	G                   = 1024 * M
	FileBufferSize      = 64 * K
	FileSortShardSize   = 2 * M
	FileMergeBufferSize = 32 * M
	InsertBatch         = 45 * K
	FileSortLimit       = 100
	SyncLimit           = 28
	LowSyncLimit        = 28
	PreparedBatch       = 3
)
