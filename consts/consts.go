package consts

const (
	//Dir = "D:\\workspace-tencent\\tmp1\\"
	Dir                 = ""
	LF                  = byte('\n')
	COMMA               = byte(',')
	K                   = 1024
	M                   = 1024 * K
	G                   = 1024 * M
	FileBufferSize      = 64 * K
	FileSortShardSize   = 8 * M
	FileMergeBufferSize = 32 * M
	InsertBatch         = 16 * K
	FileSortLimit       = 1
	SyncLimit           = 28
	PreparedBatch       = 3
)
