package service

import "github.com/google/uuid"

func (s *Service) DeleteImage(id uuid.UUID) error {
	images := []string{
		id.String() + ".jpg",
		id.String() + ".jpeg",
		id.String() + ".png",
		id.String() + ".gif",
	}

	if err := s.storage.DeleteImage(id); err != nil {
		return err
	}

	if err := s.fileStorage.DeleteImages("images", images...); err != nil {
		return err
	}

	if err := s.fileStorage.DeleteImages("processed", images...); err != nil {
		return err
	}

	return nil
}
