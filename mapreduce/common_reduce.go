package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	intermediateFiles := make([]*os.File, nMap)

	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		intermediateFiles[i], _ = os.Open(fileName)
	}

	//get all the key values from the intermediate files
	intermediateKV := make(map[string][]string)
	for _, file := range intermediateFiles {
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			intermediateKV[kv.Key] = append(intermediateKV[kv.Key], kv.Value)
		}
	}

	keys := make([]string, 0, len(intermediateKV))

	for k := range intermediateKV {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	outputFile, err := os.Create(outFile)
	checkError(err)
	defer outputFile.Close()

	enc := json.NewEncoder(outputFile)
	for _, key := range keys {
		kv := KeyValue{key, reduceF(key, intermediateKV[key])}
		enc.Encode(kv)
	}

}
