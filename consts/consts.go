package consts

const (
	//Dir = "D:\\workspace-tencent\\tmp1\\"
	Dir                 = ""
	LF                  = byte('\n')
	COMMA               = byte(',')
	K                   = 1024
	M                   = 1024 * K
	G                   = 1024 * M
	FileBufferSize      = 256 * K
	FileSortShardSize   = 32 * M
	FileMergeBufferSize = 32 * M
	InsertBatch         = 256 * K
	FileSortLimit       = 2
	SyncLimit           = 8
	PreparedBatch       = 1
)
