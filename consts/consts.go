package consts

const (
	LF                  = byte('\n')
	COMMA               = byte(',')
	K                   = 1024
	M                   = 1024 * K
	G                   = 1024 * M
	FileBufferSize      = 128 * K
	FileSortShardSize   = 512 * K
	FileMergeBufferSize = 32 * M
	InsertBatch         = 40 * K
	FileSortLimit       = 1
	SyncLimit           = 12
	PreparedBatch       = 4
)
