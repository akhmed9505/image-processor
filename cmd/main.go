package main

import (
	"log"

	"github.com/akhmed9505/image-processor/cmd/app"
)

// @title Image Processor API
// @version 1.0
// @description Image Processor API. You can send image to process with multiple settings. See methods for more information.
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @contact.email support@swagger.io

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host localhost:8080
// @BasePath /
func main() {
	if err := app.Run(); err != nil {
		log.Fatal("could not start server: ", err)
	}
}
