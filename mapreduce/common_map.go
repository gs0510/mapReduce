package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"
)

func checkError(e error) {
	if e != nil {
		panic(e)
	}
}

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	fileContents, err := ioutil.ReadFile(inFile)
	checkError(err)

	intermediateFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		filename := reduceName(jobName, mapTask, i)
		intermediateFiles[i], err = os.Create(filename)
		checkError(err)
	}

	result := mapF(inFile, string(fileContents))
	for _, entry := range result {
		index := ihash(entry.Key) % nReduce
		enc := json.NewEncoder(intermediateFiles[index])
		enc.Encode(entry)
	}

	for _, file := range intermediateFiles {
		file.Close()
	}

}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
