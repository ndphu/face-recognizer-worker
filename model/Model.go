package model

import (
	"github.com/Kagami/go-face"
	"image"
)

type RecognizedFace struct {
	Rect       image.Rectangle `json:"rect"`
	Label      string          `json:"label"`
	Classified int             `json:"category"`
	Descriptor [128]float32    `json:"descriptor"`
}

type DetectedFace struct {
	Rect       image.Rectangle `json:"rect"`
	Descriptor face.Descriptor `json:"descriptor"`
}

type RecognizeRequest struct {
	Payload   string `json:"payload"`
	RequestId string `json:"requestId"`
}

type RecognizedResponse struct {
	RecognizedFaces []RecognizedFace `json:"recognizedFaces"`
	Error           error            `json:"error"`
}

type BulkRecognizeRequest struct {
	Images    [][]byte `json:"payload"`
	RequestId string   `json:"requestId"`
}

type BulkRecognizeResponse struct {
	Labels []string `json:"labels"`
	Error  error    `json:"error"`
}

type Project struct {
	Id        string `json:"id"`
	ProjectId string `json:"projectId"`
	Name      string `json:"name"`
}
