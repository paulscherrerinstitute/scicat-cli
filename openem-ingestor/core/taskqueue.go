package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

type TaskQueue struct {
	datasetSourceFolders sync.Map           // Datastructure to store all the upload requests
	inputChannel         chan IngestionTask // Requests to upload data are put into this channel
	resultChannel        chan TaskResult    // The result of the upload is put into this channel
	AppContext           context.Context
	Config               Config
}

type TransferMethods int

const (
	TransferS3 TransferMethods = iota + 1
	TransferGlobus
)

type TransferOptions struct {
	S3_endpoint string
	S3_Bucket   string
	Md5checksum bool
}

type IngestionTask struct {
	// DatasetFolderId   uuid.UUID
	DatasetFolder     DatasetFolder
	ScicatUrl         string
	ScicatAccessToken string
	TransferMethod    TransferMethods
	TransferOptions   TransferOptions
	RequestContext    context.Context
	Cancel            context.CancelFunc
}

type TaskResult struct {
	Elapsed_seconds int
	Dataset_PID     string
	Error           error
}

func (w *TaskQueue) Startup() {

	w.inputChannel = make(chan IngestionTask)
	w.resultChannel = make(chan TaskResult)

	// start multiple go routines/workers that will listen on the input channel
	for worker := 1; worker <= w.Config.Misc.ConcurrencyLimit; worker++ {
		go w.startWorker()
	}

}

func (w *TaskQueue) CreateTask(folder DatasetFolder) error {

	task := IngestionTask{
		DatasetFolder:     folder,
		ScicatUrl:         w.Config.Scicat.Host,
		ScicatAccessToken: w.Config.Scicat.AccessToken,
		TransferMethod:    TransferS3,
		TransferOptions: TransferOptions{
			S3_endpoint: w.Config.Transfer.S3.Endpoint,
			S3_Bucket:   w.Config.Transfer.S3.Bucket,
			Md5checksum: w.Config.Transfer.S3.Checksum,
		},
	}
	_, found := w.datasetSourceFolders.Load(task.DatasetFolder.Id)
	if found {
		return errors.New("Key  exists")
	}
	w.datasetSourceFolders.Store(task.DatasetFolder.Id, task)
	return nil
}

// Go routine that listens on the channel continously for upload requests and executes uploads.
func (w *TaskQueue) startWorker() {
	for ingestionTask := range w.inputChannel {
		task_context, cancel := context.WithCancel(w.AppContext)
		defer cancel()

		ingestionTask.Cancel = cancel

		result := w.IngestDataset(task_context, ingestionTask)
		w.resultChannel <- result
	}
}

func (w *TaskQueue) CancelTask(id uuid.UUID) {
	value, found := w.datasetSourceFolders.Load(id)
	if found {
		f := value.(IngestionTask)
		if f.Cancel != nil {
			f.Cancel()
		}
		runtime.EventsEmit(w.AppContext, "upload-canceled", id)
	}
}

func (w *TaskQueue) RemoveTask(id uuid.UUID) {
	value, found := w.datasetSourceFolders.Load(id)
	if found {
		f := value.(IngestionTask)
		if f.Cancel != nil {
			f.Cancel()
		}
		w.datasetSourceFolders.Delete(id)
		runtime.EventsEmit(w.AppContext, "folder-removed", id)
	}
}

func (w *TaskQueue) ScheduleTask(id uuid.UUID) {

	value, found := w.datasetSourceFolders.Load(id)
	if !found {
		fmt.Println("Scheduling upload failed for: ", id)
		return
	}

	task := value.(IngestionTask)

	// Go routine to handle result and errors
	go func(id uuid.UUID) {
		taskResult := <-w.resultChannel
		if taskResult.Error != nil {
			runtime.EventsEmit(w.AppContext, "upload-failed", id, fmt.Sprint(taskResult.Error))
			println(fmt.Sprint(taskResult.Error))
		} else {
			runtime.EventsEmit(w.AppContext, "upload-completed", id, taskResult.Elapsed_seconds)
			println(taskResult.Dataset_PID, taskResult.Elapsed_seconds)
		}
	}(task.DatasetFolder.Id)

	// Go routine to schedule the upload asynchronously
	go func(folder DatasetFolder) {
		fmt.Println("Scheduled upload for: ", folder)
		runtime.EventsEmit(w.AppContext, "upload-scheduled", folder.Id)

		// this channel is read by the go routines that does the actual upload
		w.inputChannel <- task
	}(task.DatasetFolder)

}

func (w *TaskQueue) IngestDataset(task_context context.Context, task IngestionTask) TaskResult {
	start := time.Now()
	// TODO: add ingestion function
	// dataset_id, err := IngestDataset(task_context, w.AppContext, task)
	time.Sleep(time.Second * 5)
	datasetPID := "1"
	end := time.Now()
	elapsed := end.Sub(start)
	return TaskResult{Dataset_PID: datasetPID, Elapsed_seconds: int(elapsed.Seconds()), Error: nil}
}
