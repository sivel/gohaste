// Copyright 2014 Matt Martz <matt@sivel.net>
// All Rights Reserved.
//
//    Licensed under the Apache License, Version 2.0 (the "License"); you may
//    not use this file except in compliance with the License. You may obtain
//    a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//    License for the specific language governing permissions and limitations
//    under the License.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

type Walker struct {
	ci chan string
}

// Walk is the filepath.Walk WalkFunc to handle recording visited paths
func (w *Walker) Walk(path string, info os.FileInfo, err error) error {
	if !info.IsDir() {
		w.ci <- path
	}
	return nil
}

type AuthContainer struct {
	Auth Auth `json:"auth"`
}

type Auth struct {
	PasswordCredentials *PasswordCredentials `json:"passwordCredentials,omitempty"`
	ApiKeyCredentials   *ApiKeyCredentials   `json:"RAX-KSKEY:apiKeyCredentials,omitempty"`
	TenantId            string               `json:"tenantId,omitempty"`
	TenantName          string               `json:"tenantName,omitempty"`
}

type PasswordCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type ApiKeyCredentials struct {
	Username string `json:"username"`
	ApiKey   string `json:"apiKey"`
}

type Tokens struct {
	Access Access `json:"access"`
}

type Access struct {
	Token          Token `json:"token"`
	ServiceCatalog []CatalogEntry
}

type CatalogEntry struct {
	Name, Type string
	Endpoints  []EntryEndpoint
}

type EntryEndpoint struct {
	Region, TenantId                    string
	PublicURL, InternalURL              string
	VersionId, VersionInfo, VersionList string
}

type Token struct {
	Id      string `json:"id"`
	Expires string `json:"expires"`
}

type CloudFiles struct {
	Username  string
	ApiKey    string
	Token     string
	Endpoint  string
	Container string
	Region    string
}

// Auth will authenticate to CloudFiles
func (c *CloudFiles) Auth() {
	var tokens *Tokens

	auth := &AuthContainer{
		Auth: Auth{
			ApiKeyCredentials: &ApiKeyCredentials{
				Username: c.Username,
				ApiKey:   c.ApiKey,
			},
		},
	}
	body, _ := json.Marshal(auth)

	res, err := http.Post("https://identity.api.rackspacecloud.com/v2.0/tokens", "application/json", bytes.NewBuffer(body))
	defer res.Body.Close()
	if res.StatusCode != 200 || err != nil {
		log.Fatal("Unable to authenticate")
	}

	resBody, _ := ioutil.ReadAll(res.Body)

	json.Unmarshal(resBody, &tokens)

	for _, service := range tokens.Access.ServiceCatalog {
		if service.Type == "object-store" {
			for _, endpoint := range service.Endpoints {
				if endpoint.Region == c.Region {
					c.Endpoint = endpoint.PublicURL
					break
				}
			}
			break
		}
	}

	if len(c.Endpoint) == 0 {
		log.Fatal(fmt.Sprintf("No PublicURL found for object-store in region %s", c.Region))
	}

	c.Token = tokens.Access.Token.Id
}

func (c *CloudFiles) ListObjects(ci chan string) {
	var resBody []byte
	var marker string
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/%s", c.Endpoint, c.Container), nil)
	req.Header.Set("X-Auth-Token", c.Token)
	req.Header.Set("Accept", "text/plain")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Fatal("err != nil")
		log.Fatal(err)
	} else if res.StatusCode != 200 {
		resBody, _ = ioutil.ReadAll(res.Body)
		fmt.Println(string(resBody))
		log.Fatal(res.StatusCode)
	}
	defer res.Body.Close()
	resBody, _ = ioutil.ReadAll(res.Body)
	for _, object := range strings.Split(strings.TrimSpace(string(resBody)), "\n") {
		if len(strings.TrimSpace(object)) == 0 {
			continue
		}
		ci <- object
		marker = object
	}
	go func(ci chan string, resBody []byte, marker string) {
		for len(resBody) > 0 {
			req.URL, _ = url.ParseRequestURI(fmt.Sprintf("%s/%s?marker=%s", c.Endpoint, c.Container, marker))
			res, err := client.Do(req)
			if err != nil {
				log.Fatal(err)
			}
			resBody, _ = ioutil.ReadAll(res.Body)
			for _, object := range strings.Split(strings.TrimSpace(string(resBody)), "\n") {
				if len(strings.TrimSpace(object)) == 0 {
					continue
				}
				ci <- object
				marker = object
			}
			res.Body.Close()
		}
		close(ci)
	}(ci, resBody, marker)
}

// CreateContainer ensures that a container exists
func (c *CloudFiles) CreateContainer() {
	req, _ := http.NewRequest("PUT", fmt.Sprintf("%s/%s", c.Endpoint, c.Container), bytes.NewBuffer([]byte{}))
	req.Header.Set("X-Auth-Token", c.Token)
	client := &http.Client{}
	res, _ := client.Do(req)
	fmt.Println(res.StatusCode)
}

// Upload is a goroutine that uploads files provided by a channel to a CloudFiles container
func (c *CloudFiles) Upload(thread int, ci chan string, wg *sync.WaitGroup, BasePath string) {
	fmt.Printf("Creating uploader thread: %03d\n", thread)

	defer wg.Done()

	client := &http.Client{}
	req, _ := http.NewRequest("PUT", "", nil)
	req.Header.Set("X-Auth-Token", c.Token)

	for path := range ci {
		ObjPath := strings.TrimPrefix(strings.Replace(path, BasePath, "", 1), "/")
		fmt.Printf("Thread %03d: uploading %s\n", thread, ObjPath)

		file, err := os.Open(path)
		if err != nil {
			log.Print(fmt.Printf("%s\n", err))
			continue
		}

		req.URL, _ = url.ParseRequestURI(fmt.Sprintf("%s/%s/%s", c.Endpoint, c.Container, ObjPath))
		req.Body = file
		res, err := client.Do(req)
		if err != nil {
			log.Print(fmt.Printf("%s\n", err))
			continue
		}
		file.Close()
		res.Body.Close()
		fmt.Printf("Thread %03d: upload complete for %s\n", thread, ObjPath)
	}
	fmt.Printf("Thread %03d: exiting\n", thread)
}

func (c *CloudFiles) Delete(thread int, ci chan string, wg *sync.WaitGroup) {
	fmt.Printf("Creating deleter thread: %03d\n", thread)

	defer wg.Done()

	client := &http.Client{}
	req, _ := http.NewRequest("DELETE", "", nil)
	req.Header.Set("X-Auth-Token", c.Token)

	for path := range ci {
		fmt.Printf("Thread %03d: deleting %s\n", thread, path)
		req.URL, _ = url.ParseRequestURI(fmt.Sprintf("%s/%s/%s", c.Endpoint, c.Container, path))
		res, err := client.Do(req)
		if err != nil {
			log.Print(fmt.Printf("%s\n", err))
			continue
		}
		res.Body.Close()
		fmt.Printf("Thread %03d: delete complete for %s\n", thread, path)
	}
	fmt.Printf("Thread %03d: exiting\n", thread)
}

func (c *CloudFiles) Download(thread int, ci chan string, wg *sync.WaitGroup, BasePath string) {
	fmt.Printf("Creating downloader thread: %03d\n", thread)

	defer wg.Done()

	client := &http.Client{}
	req, _ := http.NewRequest("GET", "", nil)
	req.Header.Set("X-Auth-Token", c.Token)

	for path := range ci {
		fmt.Printf("Thread %03d: downloading %s\n", thread, path)
		FullPath := filepath.Join(BasePath, path)
		req.URL, _ = url.ParseRequestURI(fmt.Sprintf("%s/%s/%s", c.Endpoint, c.Container, path))
		res, err := client.Do(req)
		if err != nil {
			log.Print(fmt.Printf("%s\n", err))
			continue
		}

		os.MkdirAll(filepath.Dir(FullPath), 0755)
		file, err := os.Create(FullPath)
		if err != nil {
			log.Print(fmt.Printf("%s\n", err))
			continue
		}

		_, err = io.Copy(file, res.Body)
		if err != nil {
			log.Print(fmt.Printf("%s\n", err))
			continue
		}
		file.Close()
		res.Body.Close()
		fmt.Printf("Thread %03d: download complete for %s\n", thread, path)
	}
	fmt.Printf("Thread %03d: exiting\n", thread)
}

func Usage() {
	fmt.Printf(`usage: %s [options] {delete,upload,download} source [destination]

Delete:
    gohaste [options] delete my-container

Upload:
    gohaste [options] upload /path/to/files my-container

Download:
    gohaste [options] download my-container /path/to/files

options:
`, path.Base(os.Args[0]))
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	var w Walker
	var Username string
	var Password string
	var Region string
	var Concurrency int

	flag.Usage = Usage
	flag.StringVar(&Username, "username", os.Getenv("OS_USERNAME"), "Username to authenticate with. Defaults to OS_USERNAME")
	flag.StringVar(&Password, "password", os.Getenv("OS_PASSWORD"), "Password to authenticate with. Defaults to OS_PASSWORD")
	flag.StringVar(&Region, "region", os.Getenv("OS_REGION_NAME"), "Password to authenticate with. Defaults to OS_REGION_NAME")
	flag.IntVar(&Concurrency, "concurrency", 10, "Number of cuncurrent operations. Defaults to 10")
	flag.Parse()

	Operation := strings.ToLower(flag.Arg(0))
	Src := flag.Arg(1)
	Dest := flag.Arg(2)

	if len(Username) == 0 || len(Password) == 0 || len(Region) == 0 || len(Operation) == 0 || len(Src) == 0 {
		Usage()
	}

	if Operation != "upload" && Operation != "download" && Operation != "delete" {
		log.Fatal(fmt.Sprintf("%s not a supported operation", Operation))
	}

	if Operation != "delete" && len(Dest) == 0 {
		log.Fatal("'destination' is a required argument for 'upload' and 'download'")
	}

	ci := make(chan string)
	wg := new(sync.WaitGroup)

	c := CloudFiles{
		Username: Username,
		ApiKey:   Password,
		Region:   Region,
	}
	c.Auth()

	for i := 0; i < Concurrency; i++ {
		wg.Add(1)
		if Operation == "upload" {
			go c.Upload(i, ci, wg, Src)
		} else if Operation == "download" {
			Dest, _ = filepath.Abs(Dest)
			go c.Download(i, ci, wg, Dest)
		} else {
			go c.Delete(i, ci, wg)
		}
	}

	if Operation == "upload" {
		c.Container = Dest
		c.CreateContainer()
		w = Walker{ci: ci}
		filepath.Walk(Src, w.Walk)
		close(w.ci)
	} else {
		c.Container = Src
		c.ListObjects(ci)
	}

	wg.Wait()
}
