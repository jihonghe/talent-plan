cd ..

go build ./main.go && ./main -cpuProf=true -memProf=true
echo "----------------------------------------------------------------------------------------------"
echo "Now you can see the profile data in directory profile_data, copy following commands to run it:"
echo "go tool pprof cpuProf.prof"
echo "go tool pprof memProf.prof"
echo "----------------------------------------------------------------------------------------------"
