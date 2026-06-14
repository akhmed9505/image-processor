package service

import (
	"context"
	"fmt"
	"os"

	"github.com/wb-go/wbf/zlog"
)

const (
	workersNumber = 3
)

func (s *Service) StartWorkers(ctx context.Context) {

	for i := range workersNumber {
		zlog.Logger.Info().Msgf("starting worker with index: %d", i)
		go s.worker(ctx)
	}
}

func (s *Service) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			zlog.Logger.Info().Msg("recieved signal to finish worker...")
			return
		default:
			if err := s.handleMessage(); err != nil {
				zlog.Logger.Error().Msg(err.Error())
				continue
			}
			zlog.Logger.Info().Msg("successfully processed message and saved in file storage")
		}
	}
}

func (s *Service) handleMessage() error {
	message, err := s.queue.ConsumeMessage()
	if err != nil {
		return fmt.Errorf("could not consume message from queue: %s", err.Error())
	}

	oPath := originDirName + "/" + message.FileName
	pPath := processedDirName + "/" + message.FileName
	file, err := os.Create(pPath)
	if err != nil {
		return fmt.Errorf("could not creaet file to save image from storage: %s", err.Error())
	}
	file.Close()

	if err := s.ProcessImage(*message); err != nil {
		return fmt.Errorf("could not process image: %s", err.Error())
	}

	if err := s.fileStorage.SaveImage(message.FileName, pPath, "processed"); err != nil {
		return fmt.Errorf("could not save processed message to fileStorage: %s", err.Error())
	}

	if err := s.storage.UpdateImageStatus(message.ID, "finished"); err != nil {
		return fmt.Errorf("could not update image processing status in db: %s", err.Error())
	}

	if err := os.Remove(pPath); err != nil {
		return fmt.Errorf("could not delete temp file: %s", err.Error())
	}

	if err := os.Remove(oPath); err != nil {
		return fmt.Errorf("could not delete temp file: %s", err.Error())
	}

	return nil
}
