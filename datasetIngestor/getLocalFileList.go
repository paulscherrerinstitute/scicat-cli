/* scan files, extract time and owner info and statistical data*/
package datasetIngestor

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

type Datafile struct {
	Path  string `json:"path"`
	User  string `json:"uid"`
	Group string `json:"gid"`
	Perm  string `json:"perm"`
	Size  int64  `json:"size"`
	Time  string `json:"time"`
}

const windows = "windows"

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

/*
GetLocalFileList scans a source folder and optionally a file listing, and returns a list of data files, the earliest and latest modification times, the owner, the number of files, and the total size of the files.

Parameters:
- sourceFolder: The path to the source folder to scan.
- filelistingPath: The path to a file listing to use. If this is an empty string, the function scans the entire source folder.
- skip: A pointer to a string that controls how the function handles symbolic links. The string can have the following values:
  - "sA", "sa": Skip all symbolic links.
  - "kA", "ka": Keep all symbolic links.
  - "dA", "da": Keep symbolic links that point to the source folder, skip others.
  - "": The function asks the user how to handle each symbolic link.

Returns:
- fullFileArray: A slice of Datafile structs, each representing a file in the source folder or file listing.
- startTime: The earliest modification time of the files.
- endTime: The latest modification time of the files.
- owner: The owner of the files.
- numFiles: The number of files.
- totalSize: The total size of the files.

The function logs an error and returns if it cannot change the working directory to the source folder.
*/
func GetLocalFileList(sourceFolder string, filelistingPath string, symlinkCallback func(symlinkPath string, sourceFolder string) (bool, error), filenameCheckCallback func(filepath string) bool) (fullFileArray []Datafile, startTime time.Time, endTime time.Time, owner string, numFiles int64, totalSize int64, err error) {
	// scan all lines
	//fmt.Println("sourceFolder,listing:", sourceFolder, filelistingPath)
	fullFileArray = make([]Datafile, 0)
	startTime = time.Date(2500, 1, 1, 12, 0, 0, 0, time.UTC)
	endTime = time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC)
	owner = ""
	numFiles = 0
	totalSize = 0

	var lines []string

	if filelistingPath == "" {
		log.Printf("No explicit filelistingPath defined - full folder %s is used.\n", sourceFolder)
		lines = append(lines, "./")
	} else {
		lines, err = readLines(filelistingPath)
		if err != nil {
			log.Fatalf("readLines: %s", err)
		}
		for i, line := range lines {
			log.Println(i, line)
		}
	}

	// TODO verify that filelisting have no overlap, e.g. no lines X/ and X/Y,
	// because the latter is already contained in X/

	// restore oldWorkDir after function
	oldWorkDir, err := os.Getwd()
	if err != nil {
		return fullFileArray, startTime, endTime, owner, numFiles, totalSize, err
	}

	defer os.Chdir(oldWorkDir)
	// for windows source path add colon in the leading drive character
	// windowsSource := strings.Replace(sourceFolder, "/C/", "C:/", 1)
	if runtime.GOOS == windows {
		re := regexp.MustCompile(`^\/([A-Z])\/`)
		sourceFolder = re.ReplaceAllString(sourceFolder, "$1:/")
	}

	if err := os.Chdir(sourceFolder); err != nil {
		log.Printf("Can not step into sourceFolder %v - dataset will be ignored.\n", sourceFolder)
		return fullFileArray, startTime, endTime, owner, numFiles, totalSize, err
	}
	dir, err := os.Getwd()
	if err != nil {
		return fullFileArray, startTime, endTime, owner, numFiles, totalSize, err
	}
	log.Printf("Scanning source folder: %s at %s", sourceFolder, dir)

	// spin := spinner.New(spinner.CharSets[9], 100*time.Millisecond) // spinner for progress indication
	// spin.Writer = os.Stderr
	// spin.Color("green")

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		// spin.Start() // Start the spinner
		e := filepath.Walk(line, func(path string, f os.FileInfo, err error) error {
			// ignore ./ (but keep other dot files)
			if f == nil || f.Name() == "" {
				log.Printf("Skipping file or directory %s", path)
				return nil
			}
			if f.IsDir() && f.Name() == "." {
				return nil
			}

			// extract OS dependent owner IDs and translate to names
			if err != nil {
				// stop function if err given by Walk is not nil
				return err
			}
			uidName, gidName := GetFileOwner(f)
			// replace backslashes for windows path
			modpath := path
			if runtime.GOOS == windows {
				modpath = strings.Replace(path, "\\", "/", -1)
			}
			fileStruct := Datafile{Path: modpath, User: uidName, Group: gidName, Perm: f.Mode().String(), Size: f.Size(), Time: f.ModTime().Format(time.RFC3339)}
			keep := true

			// * handle symlinks *
			if f.Mode()&os.ModeSymlink != 0 {
				if symlinkCallback != nil {
					symlinkCallback(modpath, sourceFolder)
				} else {
					keep, err = handleSymlink(modpath, sourceFolder)
					if err != nil {
						return err
					}
				}
			}

			// filter invalid filenames if callback was set
			if filenameCheckCallback != nil {
				keep = keep && filenameCheckCallback(modpath)
			}

			if keep {
				numFiles++
				totalSize += f.Size()
				//fmt.Println(numFiles, totalSize)
				//fullFileArray = append(fullFileArray, fileline)
				fullFileArray = append(fullFileArray, fileStruct)
				// find out earlist creation time
				modTime := f.ModTime()
				//fmt.Printf("FileTime:", modTime)
				diff := modTime.Sub(startTime)
				if diff < (time.Duration(0) * time.Second) {
					startTime = modTime
					// fmt.Printf("Earliest Time:%v\n", startTime)
				}
				diff = modTime.Sub(endTime)
				if diff > (time.Duration(0) * time.Second) {
					endTime = modTime
					//fmt.Printf("Last Time:%v\n", endTime)
				}
				owner = gidName
			}

			return err
		})

		if e != nil {
			log.Fatal("Fatal error:", e)
		}
	}
	// spin.Stop()
	return fullFileArray, startTime, endTime, owner, numFiles, totalSize, err
}

func handleSymlink(symlinkPath string, sourceFolder string) (bool, error) {
	keep := true
	pointee, _ := os.Readlink(symlinkPath) // just pass the file name
	if !filepath.IsAbs(pointee) {
		dir, err := filepath.Abs(filepath.Dir(symlinkPath))
		if err != nil {
			keep = false
			err = fmt.Errorf("could not find absolute path of symlink at \"%s\": %v", symlinkPath, err)
			return false, err
		}
		// log.Printf(" CWD path pointee :%v %v %v", dir, filepath.Dir(path), pointee)
		pabs := filepath.Join(dir, filepath.Dir(symlinkPath), pointee)
		pointee, err = filepath.EvalSymlinks(pabs)
		if err != nil {
			keep = false
			err = fmt.Errorf("could not follow symlink: %v", err)
			return keep, err
		}
	}
	// keep symlink if it points to somewhere *within* the sourceFolder
	keep = strings.HasPrefix(pointee, sourceFolder)
	return keep, nil
}
