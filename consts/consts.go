package consts

const (
	LF                  = byte('\n')
	COMMA               = byte(',')
	K                   = 1024
	M                   = 1024 * K
	G                   = 1024 * M
	FileBufferSize      = 64 * K
	FileSortShardSize   = 4 * M
	FileMergeBufferSize = 16 * M
	InsertBatch         = 20 * K
	FileSortLimit       = 1
	SyncLimit           = 8
	PreparedBatch       = 8
)
