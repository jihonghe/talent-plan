.PHONY: all

all: test_example test_homework cleanup gendata

test_example:
	go test -v -run=TestExampleURLTop -timeout 40m

test_homework:
	go test -v -run=TestURLTop -timeout 40m

cleanup:
	go test -v -run=TestCleanData

gendata:
	go test -v -run=TestGenData
