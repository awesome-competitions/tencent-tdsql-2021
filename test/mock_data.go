package main

import (
	"fmt"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/util"
	"github.com/go-basic/uuid"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
)

func main() {
	dataPath := "D:\\workspace\\tencent\\data1"
	dataSourceFiles, err := ioutil.ReadDir(dataPath)
	if err != nil {
		panic(err)
	}
	filter := map[string]bool{}
	total := 0
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
					f, _ := file.New(util.AssemblePath(dataPath, dataSourceFile.Name(), databaseFile.Name(), tableFile.Name()), os.O_RDONLY)
					bs, _ := f.ReadAll()
					s := string(bs)
					strs := strings.Split(s, "\n")
					for _, line := range strs {
						if line == "" {
							continue
						}
						total++
						filter[line] = true
					}
				}
			}
		}
	}
	fmt.Println(len(filter))
	fmt.Println(total)
}

func _main() {
	dataPath := "D:\\workspace\\tencent\\data1"
	dataSourceFiles, err := ioutil.ReadDir(dataPath)
	if err != nil {
		panic(err)
	}
	filter := map[int]bool{}
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
					f, err := os.OpenFile(util.AssemblePath(dataPath, dataSourceFile.Name(), databaseFile.Name(), tableFile.Name()), os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0766))
					if err != nil {
						panic(err)
					}
					log.Infof("write file %s\n", tableFile.Name())
					for i := 0; i < 10000000; i++ {
						id := rand.Intn(10000*100000) + 1
						filter[id] = true
						_, _ = f.WriteString(fmt.Sprintf("%d,%f,%s,%s\n", id, rand.Float64(), strings.ReplaceAll(uuid.New(), "-", ""), "2020-12-26 09:56:37"))
					}
				}
			}
		}
	}
	fmt.Println(len(filter))
}
