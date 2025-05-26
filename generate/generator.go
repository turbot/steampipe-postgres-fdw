//go:build tool

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"text/template"
)

const templateExt = ".tmpl"

func RenderDir(templatePath, root, plugin, pluginGithubUrl, pluginVersion, pgVersion string) {
	var targetFilePath string
	err := filepath.Walk(templatePath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error accessing path %s: %v\n", filePath, err)
			return nil
		}

		fmt.Println("filePath:", filePath)
		if info.IsDir() {
			fmt.Println("not a file, continuing...\n")
			return nil
		}

		relativeFilePath := strings.TrimPrefix(filePath, root)
		// fmt.Println("relative path:", relativeFilePath)
		ext := filepath.Ext(filePath)
		// fmt.Println("extension:", ext)

		if ext != templateExt {
			fmt.Println("not tmpl, continuing...\n")
			return nil
		}

		templateFileName := strings.TrimPrefix(relativeFilePath, "/templates/")
		// fmt.Println("template fileName:", templateFileName)
		fileName := strings.TrimSuffix(templateFileName, ext)
		// fmt.Println("actual fileName:", fileName)

		targetFilePath = path.Join(root, fileName)
		// fmt.Println("targetFilePath:", targetFilePath)

		// read template file
		templateContent, err := os.ReadFile(filePath)
		if err != nil {
			fmt.Printf("Error reading template file: %v\n", err)
			return err
		}

		// create a new template and parse the content
		tmpl := template.Must(template.New(targetFilePath).Parse(string(templateContent)))

		// create a buffer to render the template
		var renderedContent strings.Builder

		// define the data to be used in the template
		data := struct {
			Plugin          string
			PluginGithubUrl string
			PluginVersion   string
			PgVersion       string
		}{
			plugin,
			pluginGithubUrl,
			pluginVersion,
			pgVersion,
		}

		// execute the template with the data
		if err := tmpl.Execute(&renderedContent, data); err != nil {
			fmt.Printf("Error rendering template: %v\n", err)
			return err
		}

		// write the rendered content to the target file
		if err := os.WriteFile(targetFilePath, []byte(renderedContent.String()), 0644); err != nil {
			fmt.Printf("Error writing to target file: %v\n", err)
			return err
		}

		return nil
	})

	if err != nil {
		fmt.Println(err)
		return
	}
}

func main() {
	// Check if the correct number of command-line arguments are provided
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run generator.go <templatePath> <root> <plugin> [plugin_version] [pluginGithubUrl]")
		return
	}

	templatePath := os.Args[1]
	root := os.Args[2]
	plugin := os.Args[3]
	var pluginVersion string
	var pluginGithubUrl string

	// Check if pluginVersion is provided as a command-line argument
	if len(os.Args) >= 5 {
		pluginVersion = os.Args[4]
	}

	// Check if PluginGithubUrl is provided as a command-line argument
	if len(os.Args) >= 6 {
		pluginGithubUrl = os.Args[5]
	} else {
		// If PluginGithubUrl is not provided, generate it based on PluginAlias
		pluginGithubUrl = "github.com/turbot/steampipe-plugin-" + plugin
	}

	// If pluginVersion is provided but pluginGithubUrl is not, error out
	if pluginVersion != "" && pluginGithubUrl == "" {
		fmt.Println("Error: plugin_github_url is required when plugin_version is specified")
		return
	}

	// Convert relative paths to absolute paths
	absTemplatePath, err := filepath.Abs(templatePath)
	if err != nil {
		fmt.Printf("Error converting templatePath to absolute path: %v\n", err)
		return
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		fmt.Printf("Error converting root to absolute path: %v\n", err)
		return
	}

	// get the postgres version used
	pgVersion := getPostgreSQLVersion()

	RenderDir(absTemplatePath, absRoot, plugin, pluginGithubUrl, pluginVersion, pgVersion)
}

func getPostgreSQLVersion() string {
	cmd := exec.Command("pg_config", "--version")
	out, err := cmd.Output()
	if err != nil {
		log.Fatalf("Failed to execute pg_config command: %s", err)
	}
	return string(out)
}
