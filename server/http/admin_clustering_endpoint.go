//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package http

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"strings"
	"sync"

	"github.com/couchbase/query/audit"
	"github.com/couchbase/query/clustering"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/prepareds"
	"github.com/couchbase/query/server"
	paramSettings "github.com/couchbase/query/server/settings"
	"github.com/couchbase/query/util"
	"github.com/gorilla/mux"
)

const (
	clustersPrefix = adminPrefix + "/clusters"
)

func (this *HttpEndpoint) registerClusterHandlers() {
	pingHandler := func(w http.ResponseWriter, req *http.Request) {
		this.wrapAPI(w, req, doPing)
	}
	configHandler := func(w http.ResponseWriter, req *http.Request) {
		this.wrapAPI(w, req, doConfig)
	}
	sslCertHandler := func(w http.ResponseWriter, req *http.Request) {
		this.wrapAPI(w, req, doSslCert)
	}
	clustersHandler := func(w http.ResponseWriter, req *http.Request) {
		this.wrapAPI(w, req, doClusters)
	}
	clusterHandler := func(w http.ResponseWriter, req *http.Request) {
		this.wrapAPI(w, req, doCluster)
	}
	nodesHandler := func(w http.ResponseWriter, req *http.Request) {
		this.wrapAPI(w, req, doNodes)
	}
	nodeHandler := func(w http.ResponseWriter, req *http.Request) {
		this.wrapAPI(w, req, doNode)
	}
	settingsHandler := func(w http.ResponseWriter, req *http.Request) {
		this.wrapAPI(w, req, doSettings)
	}
	routeMap := map[string]struct {
		handler handlerFunc
		methods []string
	}{
		adminPrefix + "/ping":                      {handler: pingHandler, methods: []string{"GET"}},
		adminPrefix + "/config":                    {handler: configHandler, methods: []string{"GET"}},
		adminPrefix + "/ssl_cert":                  {handler: sslCertHandler, methods: []string{"POST"}},
		adminPrefix + "/settings":                  {handler: settingsHandler, methods: []string{"GET", "POST"}},
		clustersPrefix:                             {handler: clustersHandler, methods: []string{"GET", "POST"}},
		clustersPrefix + "/{cluster}":              {handler: clusterHandler, methods: []string{"GET", "PUT", "DELETE"}},
		clustersPrefix + "/{cluster}/nodes":        {handler: nodesHandler, methods: []string{"GET", "POST"}},
		clustersPrefix + "/{cluster}/nodes/{node}": {handler: nodeHandler, methods: []string{"GET", "PUT", "DELETE"}},
	}

	for route, h := range routeMap {
		this.mux.HandleFunc(route, h.handler).Methods(h.methods...)
	}

}

func (this *HttpEndpoint) doConfigStore() (clustering.ConfigurationStore, errors.Error) {
	configStore := this.server.ConfigurationStore()
	if configStore == nil {
		return nil, errors.NewAdminAuthError(nil, "Failed to connect to Configuration Store")
	}
	return configStore, nil
}

func (this *HttpEndpoint) hasAdminAuth(req *http.Request) errors.Error {
	// retrieve the credentials from the request; the credentials must be specified
	// using basic authorization format. An error is returned if there is a step that
	// prevents retrieval of the credentials.
	authHdr := req.Header["Authorization"]
	if len(authHdr) == 0 {
		return errors.NewAdminAuthError(nil, "basic authorization required")
	}

	auth := authHdr[0]
	basicPrefix := "Basic "
	if !strings.HasPrefix(auth, basicPrefix) {
		return errors.NewAdminAuthError(nil, "basic authorization required")
	}

	decoded, err := base64.StdEncoding.DecodeString(auth[len(basicPrefix):])
	if err != nil {
		return errors.NewAdminDecodingError(err)
	}

	colonIndex := bytes.IndexByte(decoded, ':')
	if colonIndex == -1 {
		return errors.NewAdminAuthError(nil, "incorrect authorization header")
	}

	user := string(decoded[:colonIndex])
	password := string(decoded[colonIndex+1:])
	creds := map[string]string{user: password}

	// Attempt authorization with the cluster
	configstore, configErr := this.doConfigStore()
	if configErr != nil {
		return configErr
	}
	sslPrivs := []clustering.Privilege{clustering.PRIV_SYS_ADMIN}
	authErr := configstore.Authorize(creds, sslPrivs)
	if authErr != nil {
		return authErr
	}

	return nil
}

var pingStatus = struct {
	status string `json:"status"`
}{
	"ok",
}

func doPing(endpoint *HttpEndpoint, w http.ResponseWriter, req *http.Request, af *audit.ApiAuditFields) (interface{}, errors.Error) {
	af.EventTypeId = audit.API_ADMIN_PING
	return &pingStatus, nil
}

var localConfig struct {
	sync.Mutex
	name     string
	myConfig clustering.QueryNode
}

func doConfig(endpoint *HttpEndpoint, w http.ResponseWriter, req *http.Request, af *audit.ApiAuditFields) (interface{}, errors.Error) {
	af.EventTypeId = audit.API_ADMIN_CONFIG
	var self clustering.QueryNode

	cfgStore, cfgErr := endpoint.doConfigStore()
	if cfgErr != nil {
		return nil, cfgErr
	}
	name, er := cfgStore.WhoAmI()
	if er != nil {
		return nil, errors.NewAdminGetNodeError(er, server.GetIP(false))
	}
	if localConfig.myConfig != nil && name == localConfig.name {
		return localConfig.myConfig, nil
	}

	cm := cfgStore.ConfigurationManager()
	clusters, err := cm.GetClusters()
	if err != nil {
		return nil, err
	}

	for _, c := range clusters {
		clm := c.ClusterManager()
		queryNodes, err := clm.GetQueryNodes()
		if err != nil {
			return nil, err
		}

		for _, qryNode := range queryNodes {
			if qryNode.Name() == name {
				self = qryNode
				break
			}
		}
	}
	localConfig.Lock()
	defer localConfig.Unlock()
	localConfig.myConfig = self
	localConfig.name = name
	return localConfig.myConfig, nil
}

func doClusters(endpoint *HttpEndpoint, w http.ResponseWriter, req *http.Request, af *audit.ApiAuditFields) (interface{}, errors.Error) {
	af.EventTypeId = audit.API_ADMIN_CLUSTERS
	cfgStore, cfgErr := endpoint.doConfigStore()
	if cfgErr != nil {
		return nil, cfgErr
	}
	cm := cfgStore.ConfigurationManager()
	switch req.Method {
	case "GET":
		return cm.GetClusters()
	case "POST":
		cluster, err := getClusterFromRequest(req)
		if err != nil {
			return nil, err
		}
		af.Body = cluster
		return cfgStore.ConfigurationManager().AddCluster(cluster)
	default:
		return nil, errors.NewServiceErrorHttpMethod(req.Method)
	}
}

func doCluster(endpoint *HttpEndpoint, w http.ResponseWriter, req *http.Request, af *audit.ApiAuditFields) (interface{}, errors.Error) {
	af.EventTypeId = audit.API_ADMIN_CLUSTERS
	vars := mux.Vars(req)
	name := vars["cluster"]
	af.Cluster = name
	cfgStore, cfgErr := endpoint.doConfigStore()
	if cfgErr != nil {
		return nil, cfgErr
	}
	cluster, err := cfgStore.ClusterByName(name)
	if err != nil {
		return nil, err
	}

	switch req.Method {
	case "GET":
		return cluster, nil
	case "DELETE":
		return cfgStore.ConfigurationManager().RemoveCluster(cluster)
	default:
		return nil, errors.NewServiceErrorHttpMethod(req.Method)
	}
}

func doNodes(endpoint *HttpEndpoint, w http.ResponseWriter, req *http.Request, af *audit.ApiAuditFields) (interface{}, errors.Error) {
	af.EventTypeId = audit.API_ADMIN_CLUSTERS
	vars := mux.Vars(req)
	name := vars["cluster"]
	af.Cluster = name
	cfgStore, cfgErr := endpoint.doConfigStore()
	if cfgErr != nil {
		return nil, cfgErr
	}
	cluster, err := cfgStore.ClusterByName(name)
	if err != nil || cluster == nil {
		return cluster, err
	}
	switch req.Method {
	case "GET":
		return cluster.ClusterManager().GetQueryNodes()
	case "POST":
		node, err := getNodeFromRequest(req)
		if err != nil {
			return nil, err
		}
		af.Body = node
		return cluster.ClusterManager().AddQueryNode(node)
	default:
		return nil, errors.NewServiceErrorHttpMethod(req.Method)
	}
}

func doNode(endpoint *HttpEndpoint, w http.ResponseWriter, req *http.Request, af *audit.ApiAuditFields) (interface{}, errors.Error) {
	vars := mux.Vars(req)
	node := vars["node"]
	name := vars["cluster"]

	af.EventTypeId = audit.API_ADMIN_CLUSTERS
	af.Node = node
	af.Cluster = name

	cfgStore, cfgErr := endpoint.doConfigStore()
	if cfgErr != nil {
		return nil, cfgErr
	}
	cluster, err := cfgStore.ClusterByName(name)
	if err != nil || cluster == nil {
		return cluster, err
	}

	switch req.Method {
	case "GET":
		return cluster.QueryNodeByName(node)
	case "DELETE":
		return cluster.ClusterManager().RemoveQueryNodeByName(node)
	default:
		return nil, errors.NewServiceErrorHttpMethod(req.Method)
	}
}

// reload the ssl certificate. Only performed if the server is running https and
// the request contains basic authorization credentials that can be successfully
// authorized against the configuration store.
func doSslCert(endpoint *HttpEndpoint, w http.ResponseWriter, req *http.Request, af *audit.ApiAuditFields) (interface{}, errors.Error) {
	af.EventTypeId = audit.API_ADMIN_SSL_CERT
	if endpoint.httpsAddr == "" {
		return nil, errors.NewAdminNotSSLEnabledError()
	}

	err := endpoint.hasAdminAuth(req)
	if err != nil {
		return nil, err
	}

	// Auth clear: restart TLS listener to reload the SSL cert.
	closeErr := endpoint.CloseTLS()
	if closeErr != nil {
		return nil, errors.NewAdminEndpointError(closeErr, "error closing tls listenener")
	}

	tlsErr := endpoint.ListenTLS()
	if tlsErr != nil {
		return nil, errors.NewAdminEndpointError(tlsErr, "error starting tls listenener")
	}

	// response payload
	sslStatus := map[string]string{}
	sslStatus["status"] = "ok"
	sslStatus["keyfile"] = endpoint.keyFile
	sslStatus["certfile"] = endpoint.certFile

	return sslStatus, nil
}

func doSettings(endpoint *HttpEndpoint, w http.ResponseWriter, req *http.Request, af *audit.ApiAuditFields) (interface{}, errors.Error) {
	af.EventTypeId = audit.API_ADMIN_SETTINGS

	// Admin auth required
	err := endpoint.hasAdminAuth(req)
	if err != nil {
		return nil, err
	}

	settings := map[string]interface{}{}
	srvr := endpoint.server
	switch req.Method {
	case "GET":
		return fillSettings(settings, srvr), nil
	case "POST":
		decoder, e := getJsonDecoder(req.Body)
		if e != nil {
			return nil, e
		}
		err := decoder.Decode(&settings)
		if err != nil {
			return nil, errors.NewAdminDecodingError(err)
		}
		af.Values = settings

		if errP := server.ProcessSettings(settings, srvr); errP != nil {
			return nil, errP
		}

		return fillSettings(settings, srvr), nil
	default:
		return nil, errors.NewServiceErrorHttpMethod(req.Method)
	}
}

func fillSettings(settings map[string]interface{}, srvr *server.Server) map[string]interface{} {
	settings[paramSettings.CPUPROFILE] = srvr.CpuProfile()
	settings[paramSettings.MEMPROFILE] = srvr.MemProfile()
	settings[paramSettings.SERVICERS] = srvr.Servicers()
	settings[paramSettings.SCANCAP] = srvr.ScanCap()
	settings[paramSettings.REQUESTSIZECAP] = srvr.RequestSizeCap()
	settings[paramSettings.DEBUG] = srvr.Debug()
	settings[paramSettings.PIPELINEBATCH] = srvr.PipelineBatch()
	settings[paramSettings.PIPELINECAP] = srvr.PipelineCap()
	settings[paramSettings.MAXPARALLELISM] = srvr.MaxParallelism()
	settings[paramSettings.TIMEOUTSETTING] = srvr.Timeout()
	settings[paramSettings.KEEPALIVELENGTH] = srvr.KeepAlive()
	settings[paramSettings.LOGLEVEL] = srvr.LogLevel()
	threshold, _ := server.RequestsGetQualifier("threshold")
	settings[paramSettings.CMPTHRESHOLD] = threshold
	settings[paramSettings.CMPLIMIT] = server.RequestsLimit()
	settings[paramSettings.PRPLIMIT] = prepareds.PreparedsLimit()
	settings[paramSettings.PRETTY] = srvr.Pretty()
	settings[paramSettings.MAXINDEXAPI] = srvr.MaxIndexAPI()
	settings[paramSettings.N1QLFEATCTRL] = util.GetN1qlFeatureControl()
	settings = server.GetProfileAdmin(settings, srvr)
	settings = server.GetControlsAdmin(settings, srvr)
	return settings
}

func getClusterFromRequest(req *http.Request) (clustering.Cluster, errors.Error) {
	var cluster clustering.Cluster
	decoder, e := getJsonDecoder(req.Body)
	if e != nil {
		return nil, e
	}
	err := decoder.Decode(&cluster)
	if err != nil {
		return nil, errors.NewAdminDecodingError(err)
	}
	return cluster, nil
}

func getNodeFromRequest(req *http.Request) (clustering.QueryNode, errors.Error) {
	var node clustering.QueryNode
	decoder, e := getJsonDecoder(req.Body)
	if e != nil {
		return nil, e
	}
	err := decoder.Decode(&node)
	if err != nil {
		return nil, errors.NewAdminDecodingError(err)
	}
	return node, nil
}
