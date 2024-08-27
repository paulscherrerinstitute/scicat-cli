package core

import (
	"bufio"
	"context"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

func createLocalSymlinkCallbackForFileLister(skipSymlinks *string, skippedLinks *uint) func(symlinkPath string, sourceFolder string) (bool, error) {
	scanner := bufio.NewScanner(os.Stdin)
	return func(symlinkPath string, sourceFolder string) (bool, error) {
		keep := true
		pointee, _ := os.Readlink(symlinkPath) // just pass the file name
		if !filepath.IsAbs(pointee) {
			symlinkAbs, err := filepath.Abs(filepath.Dir(symlinkPath))
			if err != nil {
				return false, err
			}
			// log.Printf(" CWD path pointee :%v %v %v", dir, filepath.Dir(path), pointee)
			pointeeAbs := filepath.Join(symlinkAbs, pointee)
			pointee, err = filepath.EvalSymlinks(pointeeAbs)
			if err != nil {
				log.Printf("Could not follow symlink for file:%v %v", pointeeAbs, err)
				keep = false
				log.Printf("keep variable set to %v", keep)
			}
		}
		//fmt.Printf("Skip variable:%v\n", *skip)
		if *skipSymlinks == "ka" || *skipSymlinks == "kA" {
			keep = true
		} else if *skipSymlinks == "sa" || *skipSymlinks == "sA" {
			keep = false
		} else if *skipSymlinks == "da" || *skipSymlinks == "dA" {
			keep = strings.HasPrefix(pointee, sourceFolder)
		} else {
			color.Set(color.FgYellow)
			log.Printf("Warning: the file %s is a link pointing to %v.", symlinkPath, pointee)
			color.Unset()
			log.Printf(`
	Please test if this link is meaningful and not pointing 
	outside the sourceFolder %s. The default behaviour is to
	keep only internal links within a source folder.
	You can also specify that you want to apply the same answer to ALL 
	subsequent links within the current dataset, by appending an a (dA,ka,sa).
	If you want to give the same answer even to all subsequent datasets 
	in this command then specify a capital 'A', e.g. (dA,kA,sA)
	Do you want to keep the link in dataset or skip it (D(efault)/k(eep)/s(kip) ?`, sourceFolder)
			scanner.Scan()
			*skipSymlinks = scanner.Text()
			if *skipSymlinks == "" {
				*skipSymlinks = "d"
			}
			if *skipSymlinks == "d" || *skipSymlinks == "dA" {
				keep = strings.HasPrefix(pointee, sourceFolder)
			} else {
				keep = (*skipSymlinks != "s" && *skipSymlinks != "sa" && *skipSymlinks != "sA")
			}
		}
		if keep {
			color.Set(color.FgGreen)
			log.Printf("You chose to keep the link %v -> %v.\n\n", symlinkPath, pointee)
		} else {
			color.Set(color.FgRed)
			*skippedLinks++
			log.Printf("You chose to remove the link %v -> %v.\n\n", symlinkPath, pointee)
		}
		color.Unset()
		return keep, nil
	}
}

func createLocalFilenameFilterCallback(illegalFileNamesCounter *uint) func(filepath string) bool {
	return func(filepath string) (keep bool) {
		keep = true
		// make sure that filenames do not contain characters like "\" or "*"
		if strings.ContainsAny(filepath, "*\\") {
			color.Set(color.FgRed)
			log.Printf("Warning: the file %s contains illegal characters like *,\\ and will not be archived.", filepath)
			color.Unset()
			if illegalFileNamesCounter != nil {
				*illegalFileNamesCounter++
			}
			keep = false
		}
		// and check for triple blanks, they are used to separate columns in messages
		if keep && strings.Contains(filepath, "   ") {
			color.Set(color.FgRed)
			log.Printf("Warning: the file %s contains 3 consecutive blanks which is not allowed. The file not be archived.", filepath)
			color.Unset()
			if illegalFileNamesCounter != nil {
				*illegalFileNamesCounter++
			}
			keep = false
		}
		return keep
	}
}

// Progress notifier object for Minio upload
type MinioProgressNotifier struct {
	total_file_size     int64
	current_size        int64
	files_count         int
	current_file        int
	ctx                 context.Context
	previous_percentage float64
	start_time          time.Time
	id                  string
}

// Callback that gets called by fputobject.
// Note: does not work for multipart uploads
func (pn MinioProgressNotifier) Read(p []byte) (n int, err error) {
	n = len(p)
	pn.current_size += int64(n)

	current_percentage := float64(pn.current_size) / float64(pn.total_file_size)
	runtime.EventsEmit(pn.ctx, "progress-update", pn.id, current_percentage, pn.current_file, pn.files_count, time.Since(pn.start_time).Seconds())
	return
}

// Upload all files in a folder to a minio bucket
func UploadS3(task_ctx context.Context, app_ctx context.Context, dataset_pid string, datasetSourceFolder string, uploadId string, options TransferOptions) (string, error) {
	accessKeyID := "minio_user"
	secretAccessKey := "minio_pass"
	creds := credentials.NewStaticV4(accessKeyID, secretAccessKey, "")
	useSSL := false

	log.Printf("Using endpoint %s\n", options.S3_endpoint)

	// Initialize minio client object.
	minioClient, err := minio.New(options.S3_endpoint, &minio.Options{
		Creds:  creds,
		Secure: useSSL,
	})

	if err != nil {
		log.Fatalln(err)
	}

	// Make a new bucket called testbucket.
	bucketName := options.S3_Bucket
	

	err = minioClient.MakeBucket(task_ctx, bucketName, minio.MakeBucketOptions{Region: options.location})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(task_ctx, bucketName)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", bucketName)
		} else {
			log.Fatalln(err)
		}
	} else {
		log.Printf("Successfully created %s\n", bucketName)
	}

	contentType := "application/octet-stream"

	entries, err := os.ReadDir(datasetSourceFolder)
	if err != nil {
		return "", err
	}

	pn := MinioProgressNotifier{files_count: len(entries), ctx: app_ctx, previous_percentage: 0.0, start_time: time.Now(), id: uploadId}

	for idx, f := range entries {
		select {
		case <-task_ctx.Done():
			runtime.EventsEmit(app_ctx, "upload-canceled", uploadId)
			return "Upload canceled", nil

		default:
			filePath := path.Join(datasetSourceFolder, f.Name())
			objectName := "openem-network/datasets/" + dataset_pid + "/raw_files/" + f.Name()

			pn.current_file = idx + 1
			fileinfo, _ := os.Stat(filePath)
			pn.total_file_size = fileinfo.Size()

			// log.Printf("progress: %d of %d", pn.current_file, pn.files_count)

			runtime.EventsEmit(app_ctx, "progress-update", uploadId, 0.0, pn.current_file, pn.files_count)

			_, err := minioClient.FPutObject(task_ctx, bucketName, objectName, filePath, minio.PutObjectOptions{
				ContentType:           contentType,
				Progress:              pn,
				SendContentMd5:        true,
				NumThreads:            4,
				DisableMultipart:      false,
				ConcurrentStreamParts: true,
			})
			if err != nil {
				log.Printf(err.Error())
			}
		}
	}

	elapsed := math.Floor(time.Since(pn.start_time).Seconds())
	runtime.EventsEmit(app_ctx, "upload-completed", uploadId, elapsed)
	return dataset_pid, nil
}
