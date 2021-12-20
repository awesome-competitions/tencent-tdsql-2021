package main

import (
	"fmt"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/util"
	"github.com/go-basic/uuid"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
)

func main() {
	dataPath := "D:\\workspace\\tencent\\data"
	dataSourceFiles, err := ioutil.ReadDir(dataPath)
	if err != nil {
		panic(err)
	}
	for _, dataSourceFile := range dataSourceFiles {
		databaseFiles, err := ioutil.ReadDir(util.AssemblePath(dataPath, dataSourceFile.Name()))
		if err != nil {
			panic(err)
		}
		for _, databaseFile := range databaseFiles {
			tableFiles, err := ioutil.ReadDir(util.AssemblePath(dataPath, dataSourceFile.Name(), databaseFile.Name()))
			if err != nil {
				panic(err)
			}
			for _, tableFile := range tableFiles {
				if strings.HasSuffix(tableFile.Name(), ".csv") {
					f, err := os.OpenFile(util.AssemblePath(dataPath, dataSourceFile.Name(), databaseFile.Name(), tableFile.Name()), os.O_RDWR|os.O_APPEND, os.FileMode(0766))
					if err != nil {
						panic(err)
					}
					log.Infof("write file %s\n", tableFile.Name())
					for i := 0; i < 3000000; i++ {
						_, _ = f.WriteString(fmt.Sprintf("%d,%f,%s,%s\n", rand.Intn(1000000000), rand.Float64(), strings.ReplaceAll(uuid.New(), "-", ""), "2020-12-26 09:56:37"))
					}
				}
			}
		}
	}

}
