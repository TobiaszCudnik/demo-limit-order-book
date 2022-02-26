// https://github.com/Kautenja/limit-order-book/blob/master/notes/lob.md

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/TobiaszCudnik/order_book_exercise/lob"
)

func main() {
	// read the file and get the output via a channel
	ch := readInput("input_file.csv")

	for {
		output, ok := <-ch
		if !ok {
			break
		}
		// print output lines
		for _, line := range output {
			fmt.Println(line)
		}
	}
}

func readInput(filename string) chan []string {
	f, err := os.Open(filename)

	if err != nil {
		panic(err)
	}

	ch := make(chan []string)
	r := csv.NewReader(f)
	b := lob.NewLimitOrderBook()

	go func() {
		for {
			r, err := r.Read()
			if err == io.EOF {
				// the end
				close(ch)
				break
			}

			if err != nil {
				// ignore "wrong number of fields"
				if !strings.Contains(err.Error(), "wrong number of fields") {
					log.Println(r)
					log.Println(err)
					continue
				}
			}

			// comment
			if r[0][0] == '#' {
				if len(r[0]) > 5 && r[0][0:6] == "#name:" {
					fmt.Println(r[0])
				}
				continue
			}

			if len(os.Getenv("DEBUG")) > 0 {
				fmt.Printf("%+v\n", r)
			}

			var output []string

			switch r[0] {
			case "N":
				uid, _ := strconv.Atoi(strings.Trim(r[1], " "))
				price, _ := strconv.Atoi(strings.Trim(r[3], " "))
				qty, _ := strconv.Atoi(strings.Trim(r[4], " "))
				isSell := strings.Trim(r[5], " ") == "S"
				id, _ := strconv.Atoi(strings.Trim(r[6], " "))
				output, err = b.NewOrder(uid, price, qty, isSell, id)
				if err != nil {
					panic(err)
				}
			case "C":
				id, _ := strconv.Atoi(strings.Trim(r[2], " "))
				output, err = b.CancelOrder(id)
				if err != nil {
					panic(err)
				}
			case "F":
				b = lob.NewLimitOrderBook()
			default:
				log.Println("Unknown op ", r[0])
			}

			ch <- output
		}
	}()

	return ch
}
