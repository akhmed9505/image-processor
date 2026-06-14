package minio

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/akhmed9505/image-processor/internal/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Minio struct {
	client *minio.Client
}

func New() *Minio {
	endpoint := config.Cfg.Minio.Host + config.Cfg.Minio.Port
	accessKeyID := os.Getenv("MINIO_USER")
	secretAccessKey := os.Getenv("MINIO_PASSWORD")

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatal("could not connect to minio server: ", err)
	}

	ctx := context.Background()
	m := Minio{
		client: minioClient,
	}

	originalExists, _ := minioClient.BucketExists(ctx, "images")
	processedExists, _ := minioClient.BucketExists(ctx, "processed")

	if !originalExists {
		err = minioClient.MakeBucket(ctx, "images", minio.MakeBucketOptions{Region: "ru-moscow"})
		if err != nil {
			log.Fatal("could not create bucket in minio to save images: ", err)
		}
	}

	if !processedExists {
		err = minioClient.MakeBucket(ctx, "processed", minio.MakeBucketOptions{Region: "ru-moscow"})
		if err != nil {
			log.Fatal("could not create bucket in minio to save processed images: ", err)
		}
	}

	return &m
}

func (m *Minio) SaveImage(fileName, filePath, bucketName string) error {
	_, err := m.client.FPutObject(
		context.Background(),
		bucketName,
		fileName,
		filePath,
		minio.PutObjectOptions{},
	)
	if err != nil {
		return fmt.Errorf("could not save image in minio: %w", err)
	}

	return nil
}

func (m *Minio) GetImage(fileName, filePath, bucketName string) error {
	err := m.client.FGetObject(
		context.Background(),
		bucketName,
		fileName,
		filePath,
		minio.GetObjectOptions{},
	)
	if err != nil {
		return fmt.Errorf("could not get image from minio server: %w", err)
	}

	return nil
}

func (m *Minio) DeleteImages(bucketName string, fileNames ...string) error {
	for _, fileName := range fileNames {
		err := m.client.RemoveObject(
			context.Background(),
			bucketName,
			fileName,
			minio.RemoveObjectOptions{ForceDelete: true},
		)
		if err != nil {
			return fmt.Errorf("could not delete image from minio: %w", err)
		}
	}

	return nil
}
