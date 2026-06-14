package dto

import "github.com/google/uuid"

type Message struct {
	ID            uuid.UUID `json:"id"`
	FileName      string    `json:"file_name"`
	ContentType   string    `json:"content_type"`
	WatermarkText string    `json:"watermark_string"`
	Task          string    `json:"task"`
	Resize        struct {
		Width  int `json:"width"`
		Height int `json:"height"`
	} `json:"resize"`
}
