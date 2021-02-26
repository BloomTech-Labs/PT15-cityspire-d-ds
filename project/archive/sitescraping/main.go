package main

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"

	"github.com/danoand/utils"
)

func main() {
	var err error
	var fle *os.File
	var tmpRows [][]string
	var outRows [][]string

	fmt.Printf("INFO: start processing\n")

	rgxWlkScrPNG := regexp.MustCompile(`pp\.walk\.sc\/badge\/walk\/score\/\d+.png`)
	rgxNonDigits := regexp.MustCompile(`\D`)

	// Open the city file for reading
	fle, err = os.Open("input_cities.csv")
	if err != nil {
		// error opening up the cities file
		log.Fatalf("FATAL: %v - error opening up the cities file - see: %v\n",
			utils.FileLine(),
			err)
	}

	// Set up a csv reader used to read csv data
	rdr := csv.NewReader(fle)
	// Read in all of the csv data
	rdr.FieldsPerRecord = 4
	tmpRows, err = rdr.ReadAll()
	if err != nil {
		// error reading in the csv file data
		log.Fatalf("FATAL: %v - error reading in the csv file data - see: %v\n",
			utils.FileLine(),
			err)
	}

	// Iterate through the data records
	for _, row := range tmpRows {
		ste := row[2]
		cty := row[3]

		// Construct the url
		url := fmt.Sprintf("https://www.walkscore.com/%v/%v", ste, cty)

		// Fetch the page referenced by the url
		rsp, err := http.Get(url)
		if err != nil {
			// error fetching a web page
			log.Printf("ERROR: %v - error fetching a web page: %v - see: %v\n",
				utils.FileLine(),
				url,
				err)
			continue
		}

		// Grab the body of the response
		tmpBody, err := ioutil.ReadAll(rsp.Body)
		if err != nil {
			// error extracting the response body
			log.Printf("ERROR: %v - error extracting the response body for url: %v - see: %v\n",
				utils.FileLine(),
				url,
				err)
			continue
		}
		if len(tmpBody) == 0 {
			// error - empty response body
			log.Printf("ERROR: %v - error - empty response body\n",
				utils.FileLine())
			continue
		}

		// Match the walkability score pattern
		fndBadge := rgxWlkScrPNG.Find(tmpBody)

		// Strip out the numeric characters
		scr := rgxNonDigits.ReplaceAll(fndBadge, []byte{})
		if len(scr) == 0 {
			scr = []byte("string not found")
		}

		outRows = append(outRows, []string{row[0], ste, cty, string(scr)})
	}

	// Print out the outRows
	for i := 0; i < len(outRows); i++ {
		fmt.Printf("%v,%v,%v,%v\n", outRows[i][0], outRows[i][1], outRows[i][2], outRows[i][3])
	}

	fmt.Printf("INFO: stop processing\n")
}
