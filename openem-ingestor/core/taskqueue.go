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
	errorChannel         chan string        // the go routine puts the error of the upload here
	resultChannel        chan TaskResult    // The result of the upload is put into this channel
	AppContext           context.Context
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
}

func (w *TaskQueue) Startup(numWorkers int) {

	w.inputChannel = make(chan IngestionTask)
	w.resultChannel = make(chan TaskResult)

	// start multiple go routines/workers that will listen on the input channel
	for worker := 1; worker <= numWorkers; worker++ {
		go w.startWorker()
	}

}

func (w *TaskQueue) CreateTask(folder DatasetFolder) error {

	task := IngestionTask{
		DatasetFolder:     folder,
		ScicatUrl:         "http://scopem-openem.ethz.ch:89/api/v3",
		ScicatAccessToken: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2Njk3N2UxMWFhZTUwOWI4YzRiMjQwZTciLCJ1c2VybmFtZSI6ImluZ2VzdG9yIiwiZW1haWwiOiJzY2ljYXRpbmdlc3RvckB5b3VyLnNpdGUiLCJhdXRoU3RyYXRlZ3kiOiJsb2NhbCIsIl9fdiI6MCwiaWQiOiI2Njk3N2UxMWFhZTUwOWI4YzRiMjQwZTciLCJpYXQiOjE3MjM3MDY3MjYsImV4cCI6MTcyMzc0MjcyNn0.p0nlcM_hXoSJMsom36oPXZbknwKDsydWCyQytFLkLT4",
		TransferMethod:    TransferS3,
		TransferOptions: TransferOptions{
			S3_endpoint: "scopem-openem.ethz.ch:9000",
			S3_Bucket:   "landingzone",
			Md5checksum: true,
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

		result, err := w.IngestDataset(task_context, ingestionTask)
		if err != nil {
			w.errorChannel <- err.Error()
		}
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
		select {
		case taskResult := <-w.resultChannel:
			runtime.EventsEmit(w.AppContext, "upload-completed", id, taskResult.Elapsed_seconds)
			println(taskResult.Dataset_PID, taskResult.Elapsed_seconds)
		case err := <-w.errorChannel:
			println(err)
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

func (w *TaskQueue) IngestDataset(task_context context.Context, task IngestionTask) (TaskResult, error) {
	start := time.Now()
	// TODO: add ingestion function
	// dataset_id, err := IngestDataset(task_context, w.AppContext, task)
	time.Sleep(time.Second * 5)
	dataset_id := "1"
	end := time.Now()
	elapsed := end.Sub(start)
	return TaskResult{Dataset_PID: dataset_id, Elapsed_seconds: int(elapsed.Seconds())}, nil
}
