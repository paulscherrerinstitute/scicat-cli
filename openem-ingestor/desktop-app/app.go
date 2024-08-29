package main

import (
	"context"
	"openem-ingestor/core"

	"github.com/google/uuid"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// App struct
type App struct {
	ctx       context.Context
	taskqueue core.TaskQueue
	config    core.Config
}

// NewApp creates a new App application struct
func NewApp(config core.Config) *App {
	return &App{config: config, taskqueue: core.TaskQueue{Config: config}}
}

// Show prompt before closing the app
func (b *App) beforeClose(ctx context.Context) (prevent bool) {
	dialog, err := runtime.MessageDialog(ctx, runtime.MessageDialogOptions{
		Type:    runtime.QuestionDialog,
		Title:   "Quit?",
		Message: "Are you sure you want to quit? This will stop all pending downloads.",
	})

	if err != nil {
		return false
	}
	return dialog != "Yes"
}

// startup is called when the app starts. The context is saved
// so we can call the runtime methods
func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	a.taskqueue.AppContext = a.ctx
	a.taskqueue.Startup()
}

func (a *App) SelectFolder() {
	folder, err := core.SelectFolder(a.ctx)
	if err != nil {
		return
	}

	err = a.taskqueue.CreateTask(folder)
	if err != nil {
		return
	}
}

func (a *App) CancelTask(id uuid.UUID) {
	a.taskqueue.CancelTask(id)
}
func (a *App) RemoveTask(id uuid.UUID) {
	a.taskqueue.RemoveTask(id)
}

func (a *App) ScheduleTask(id uuid.UUID) {

	a.taskqueue.ScheduleTask(id)
}
