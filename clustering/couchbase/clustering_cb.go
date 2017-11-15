//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package clustering_cb

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/query/accounting"
	"github.com/couchbase/query/clustering"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/server"
	"github.com/couchbase/query/server/http"
)

const _PREFIX = "couchbase:"

///////// Notes about Couchbase implementation of Clustering API
//
// clustering_cb (this package) -> go-couchbase -> couchbase cluster
//
// pool is a synonym for cluster
//

// cbConfigStore implements clustering.ConfigurationStore
type cbConfigStore struct {
	sync.RWMutex
	adminUrl     string
	ourPorts     map[string]int
	noMoreChecks bool
	poolName     string
	poolSrvRev   int
	whoAmI       string
	state        clustering.Mode
	cbConn       *couchbase.Client
}

// create a cbConfigStore given the path to a couchbase instance
func NewConfigstore(path string) (clustering.ConfigurationStore, errors.Error) {
	if strings.HasPrefix(path, _PREFIX) {
		path = path[len(_PREFIX):]
	}
	c, err := couchbase.ConnectWithAuth(path, cbauth.NewAuthHandler(nil))
	if err != nil {
		return nil, errors.NewAdminConnectionError(err, path)
	}
	return &cbConfigStore{
		adminUrl:     path,
		ourPorts:     map[string]int{},
		noMoreChecks: false,
		poolSrvRev:   -999,
		cbConn:       &c,
	}, nil
}

// Implement Stringer interface
func (this *cbConfigStore) String() string {
	return fmt.Sprintf("url=%v", this.adminUrl)
}

// Implement clustering.ConfigurationStore interface
func (this *cbConfigStore) Id() string {
	return this.URL()
}

func (this *cbConfigStore) URL() string {
	return this.adminUrl
}

func (this *cbConfigStore) SetOptions(httpAddr, httpsAddr string) errors.Error {
	if httpAddr != "" {
		_, port := server.HostNameandPort(httpAddr)
		if port != "" {
			portNum, err := strconv.Atoi(port)
			if err == nil && portNum > 0 {
				this.ourPorts[_HTTP] = portNum
			} else {
				return errors.NewAdminBadServicePort(port)
			}
		} else {
			return errors.NewAdminBadServicePort("<no port>")
		}
	}
	if httpsAddr != "" {
		_, port := server.HostNameandPort(httpsAddr)
		if port != "" {
			portNum, err := strconv.Atoi(port)
			if err == nil && portNum > 0 {
				this.ourPorts[_HTTPS] = portNum
			} else {
				return errors.NewAdminBadServicePort(port)
			}
		} else {
			return errors.NewAdminBadServicePort("<no port>")
		}
	}
	return nil
}

func (this *cbConfigStore) ClusterNames() ([]string, errors.Error) {
	clusterIds := []string{}
	for _, pool := range this.getPools() {
		clusterIds = append(clusterIds, pool.Name)
	}
	return clusterIds, nil
}

func (this *cbConfigStore) ClusterByName(name string) (clustering.Cluster, errors.Error) {
	_, err := this.cbConn.GetPool(name)
	if err != nil {
		return nil, errors.NewAdminGetClusterError(err, name)
	}
	return &cbCluster{
		configStore:    this,
		ClusterName:    name,
		ConfigstoreURI: this.URL(),
		DatastoreURI:   this.URL(),
	}, nil
}

func (this *cbConfigStore) ConfigurationManager() clustering.ConfigurationManager {
	return this
}

// Helper method to retrieve all pools
func (this *cbConfigStore) getPools() []couchbase.RestPool {
	return this.cbConn.Info.Pools
}

// Helper method to retrieve Couchbase services data (/pools/default/nodeServices)
// and Couchbase pool (cluster) data (/pools/default)
//
func (this *cbConfigStore) getPoolServices(name string) (*couchbase.Pool, *couchbase.PoolServices, errors.Error) {
	nodeServices, err := this.cbConn.GetPoolServices(name)
	if err != nil {
		return nil, nil, errors.NewAdminGetClusterError(err, name)
	}

	pool, err := this.cbConn.GetPool(name)
	if err != nil {
		return nil, nil, errors.NewAdminGetClusterError(err, name)
	}

	return &pool, &nodeServices, nil
}

// cbConfigStore also implements clustering.ConfigurationManager interface
func (this *cbConfigStore) ConfigurationStore() clustering.ConfigurationStore {
	return this
}

func (this *cbConfigStore) AddCluster(l clustering.Cluster) (clustering.Cluster, errors.Error) {
	// NOP. This is a read-only implementation
	return nil, nil
}

func (this *cbConfigStore) RemoveCluster(l clustering.Cluster) (bool, errors.Error) {
	// NOP. This is a read-only implementation
	return false, nil
}

func (this *cbConfigStore) RemoveClusterByName(name string) (bool, errors.Error) {
	// NOP. This is a read-only implementation
	return false, nil
}

func (this *cbConfigStore) GetClusters() ([]clustering.Cluster, errors.Error) {
	clusters := []clustering.Cluster{}
	clusterNames, _ := this.ClusterNames()
	for _, name := range clusterNames {
		cluster, err := this.ClusterByName(name)
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, cluster)
	}
	return clusters, nil
}

func (this *cbConfigStore) Authorize(credentials map[string]string, privileges []clustering.Privilege) errors.Error {
	if len(credentials) == 0 {
		return errors.NewAdminAuthError(nil, "no credentials provided")
	}

	for username, password := range credentials {
		auth, err := cbauth.Auth(username, password)
		if err != nil {
			return errors.NewAdminAuthError(err, "unable to authenticate with given credential")
		}
		for _, requested := range privileges {
			switch requested {
			case clustering.PRIV_SYS_ADMIN:
				isAdmin, err := auth.IsAllowed("cluster.settings!write")
				if err != nil {
					return errors.NewAdminAuthError(err, "")
				}
				if isAdmin {
					return nil
				}
				return errors.NewAdminAuthError(nil, "sys admin requires administrator credentials")
			case clustering.PRIV_READ:
				isPermitted, err := auth.IsAllowed("cluster.settings!read")
				if err != nil {
					return errors.NewAdminAuthError(err, "")
				}
				if isPermitted {
					return nil
				}
				return errors.NewAdminAuthError(nil, "read not authorized")
			default:
				return errors.NewAdminAuthError(nil, fmt.Sprintf("unexpected authorization %v", requested))
			}
		}
	}
	return errors.NewAdminAuthError(nil, "unrecognized authorization request")
}

const n1qlService = "n1ql"

func (this *cbConfigStore) WhoAmI() (string, errors.Error) {
	name, state, err := this.doNameState()
	if err != nil {
		return "", err
	}
	if state != clustering.STANDALONE {
		return name, nil
	}
	return "", nil
}

func (this *cbConfigStore) State() (clustering.Mode, errors.Error) {
	_, state, err := this.doNameState()
	if err != nil {
		return "", err
	}
	return state, nil
}

func (this *cbConfigStore) doNameState() (string, clustering.Mode, errors.Error) {

	// once things get to a certain state, no changes are possible
	// hence we can skip the tests and we don't even require a lock
	if this.noMoreChecks {
		return this.whoAmI, this.state, nil
	}

	// this will exhaust all possibilities in the hope of
	// finding a good name and only return an error
	// if we could not find a name at all
	var err errors.Error

	this.RLock()

	// have we been here before?
	if this.poolName != "" {
		pool, poolServices, newErr := this.getPoolServices(this.poolName)
		if err != nil {
			err = errors.NewAdminConnectionError(newErr, this.poolName)
		} else {

			// If pool services rev matches the cluster's rev, nothing has changed
			if poolServices.Rev == this.poolSrvRev {
				defer this.RUnlock()
				return this.whoAmI, this.state, nil
			}

			// check that things are still valid
			whoAmI, state, newErr := this.checkPoolServices(pool, poolServices)
			if newErr == nil && state != "" {

				// promote the lock for the update
				// (this may be wasteful, but we have no other choice)
				this.RUnlock()
				this.Lock()
				defer this.Unlock()

				// we got here first, update
				if poolServices.Rev > this.poolSrvRev {
					this.whoAmI = whoAmI
					this.state = state

					hostName, _ := server.HostNameandPort(whoAmI)

					// no more changes will happen if we are clustered and have a FQDN
					// (name will not fall back to 127.0.0.1), or we are standalone
					this.noMoreChecks = (state == clustering.STANDALONE ||
						(state == clustering.CLUSTERED && hostName != server.GetIP(false)))

				}
				return this.whoAmI, this.state, nil
			}
		}
	}

	// Either things went badly wrong, or we are here for the first time
	// We have to work out things from first principles, which requires
	// promoting the lock
	// Somebody might have done the work while we were waiting, if so
	// we will take advantage of that
	this.RUnlock()
	this.Lock()
	defer this.Unlock()

	if this.noMoreChecks {
		return this.whoAmI, this.state, nil
	}

	if this.poolName != "" {
		pool, poolServices, newErr := this.getPoolServices(this.poolName)

		if poolServices.Rev == this.poolSrvRev {
			return this.whoAmI, this.state, nil
		}

		whoAmI, state, newErr := this.checkPoolServices(pool, poolServices)
		if newErr == nil && state != "" {
			this.whoAmI = whoAmI
			this.state = state

			hostName, _ := server.HostNameandPort(whoAmI)

			this.noMoreChecks = (state == clustering.STANDALONE ||
				(state == clustering.CLUSTERED && hostName != server.GetIP(false)))
			return this.whoAmI, this.state, nil
		} else if err == nil {
			err = newErr
		}
	}

	// nope - start from scratch
	this.whoAmI = ""
	this.state = ""
	this.poolName = ""
	this.noMoreChecks = false

	// same process, but scan all pools now
	for _, p := range this.getPools() {
		pool, poolServices, newErr := this.getPoolServices(p.Name)
		if err != nil && newErr != nil {
			err = newErr
		}
		whoAmI, state, newErr := this.checkPoolServices(pool, poolServices)
		if newErr != nil {
			if err == nil {
				err = newErr
			}
			continue
		}

		// not in this pool
		if state == "" {
			continue
		}

		this.poolName = p.Name
		this.whoAmI = whoAmI
		this.state = state

		hostName, _ := server.HostNameandPort(whoAmI)

		this.noMoreChecks = (state == clustering.STANDALONE ||
			(state == clustering.CLUSTERED && hostName != server.GetIP(false)))
		return this.whoAmI, this.state, nil
	}

	// We haven't found ourselves in there.
	// It could be we are not part of a cluster.
	// It could be ns_server is not yet listing us
	// Either way, we can't cache anything.
	return "", clustering.STARTING, err
}

func (this *cbConfigStore) checkPoolServices(pool *couchbase.Pool, poolServices *couchbase.PoolServices) (string, clustering.Mode, errors.Error) {
	for _, node := range poolServices.NodesExt {

		// the assumption is that a n1ql node is started by the local mgmt service
		// so we only have to have a look at ThisNode
		if !node.ThisNode {
			continue
		}

		// In the node services endpoint, nodes will either have a fully-qualified
		// domain name or the hostname will not be provided indicating that the
		// hostname is 127.0.0.1
		hostname := node.Hostname
		if hostname == "" {
			// For constructing URLs with raw IPv6 addresses- the IPv6 address
			// must be enclosed within ‘[‘ and ‘]’ brackets.
			hostname = server.GetIP(true)
		}

		mgmtPort := node.Services["mgmt"]
		if mgmtPort == 0 {

			// shouldn't happen, there should always be a mgmt port on each node
			// we should return an error
			msg := fmt.Sprintf("NodeServices does not report mgmt endpoint for "+
				"this node: %v", node)
			return "", "", errors.NewAdminGetNodeError(nil, msg)
		}

		// now that we have identified the node, is n1ql actually running?
		found := 0
		for serv, proto := range n1qlProtocols {
			port, ok := node.Services[serv]
			ourPort, ook := this.ourPorts[proto]

			// ports matching, good
			// port not listed, skip
			// we are not listening or ports mismatching, standalone
			if ok {
				if ook && port == ourPort {
					found++
				} else {
					return "", clustering.STANDALONE, nil
				}
			}
		}

		// we found no n1ql service port - is n1ql provisioned in this node?
		if found == 0 {
			for _, node := range pool.Nodes {
				for _, s := range node.Services {

					// yes, but clearly, not yet advertised
					// place ourselves in a holding pattern
					if s == n1qlService {
						return "", clustering.STARTING, nil
					}
				}
			}
		}

		// We don't assume that there is precisely one query node per host.
		// Query nodes are unique per mgmt endpoint, so we add the mgmt
		// port to the whoami string to uniquely identify the query node.
		whoAmI := hostname + ":" + strconv.Itoa(mgmtPort)
		return whoAmI, clustering.CLUSTERED, nil
	}
	return "", "", nil
}

// Type services associates a protocol with a port number
type services map[string]int

const (
	_HTTP  = "http"
	_HTTPS = "https"
)

// n1qlProtocols associates Couchbase query service names with a protocol
var n1qlProtocols = map[string]string{
	"n1ql":    _HTTP,
	"n1qlSSL": _HTTPS,
}

// cbCluster implements clustering.Cluster
type cbCluster struct {
	sync.Mutex
	configStore    clustering.ConfigurationStore `json:"-"`
	dataStore      datastore.Datastore           `json:"-"`
	acctStore      accounting.AccountingStore    `json:"-"`
	ClusterName    string                        `json:"name"`
	DatastoreURI   string                        `json:"datastore"`
	ConfigstoreURI string                        `json:"configstore"`
	AccountingURI  string                        `json:"accountstore"`
	version        clustering.Version            `json:"-"`
	VersionString  string                        `json:"version"`
	queryNodeNames []string                      `json:"-"`
	queryNodes     map[string]services           `json:"-"`
	poolSrvRev     int                           `json:"-"`
}

// Create a new cbCluster instance
func NewCluster(name string,
	version clustering.Version,
	configstore clustering.ConfigurationStore,
	datastore datastore.Datastore,
	acctstore accounting.AccountingStore) (clustering.Cluster, errors.Error) {
	c := makeCbCluster(name, version, configstore, datastore, acctstore)
	return c, nil
}

func makeCbCluster(name string,
	version clustering.Version,
	cs clustering.ConfigurationStore,
	ds datastore.Datastore,
	as accounting.AccountingStore) clustering.Cluster {
	cluster := cbCluster{
		configStore:    cs,
		dataStore:      ds,
		acctStore:      as,
		ClusterName:    name,
		DatastoreURI:   ds.URL(),
		ConfigstoreURI: cs.URL(),
		AccountingURI:  as.URL(),
		version:        version,
		VersionString:  version.String(),
		queryNodeNames: []string{},
		poolSrvRev:     -999,
	}
	return &cluster
}

// cbCluster implements Stringer interface
func (this *cbCluster) String() string {
	return getJsonString(this)
}

// cbCluster implements clustering.Cluster interface
func (this *cbCluster) ConfigurationStoreId() string {
	return this.configStore.Id()
}

func (this *cbCluster) Name() string {
	return this.ClusterName
}

func (this *cbCluster) QueryNodeNames() ([]string, errors.Error) {
	queryNodeNames := []string{}

	// Get a handle of the go-couchbase connection:
	configStore, ok := this.configStore.(*cbConfigStore)
	if !ok {
		return nil, errors.NewAdminConnectionError(nil, this.ConfigurationStoreId())
	}

	poolServices, err := configStore.cbConn.GetPoolServices(this.ClusterName)
	if err != nil {
		return queryNodeNames, errors.NewAdminConnectionError(err, this.ConfigurationStoreId())
	}

	// If pool services rev matches the cluster's rev, return cluster's query node names:
	if poolServices.Rev == this.poolSrvRev {
		return this.queryNodeNames, nil
	}

	// If pool services and cluster rev do not match, update the cluster's rev and query node data:
	queryNodeNames = []string{}
	queryNodes := map[string]services{}
	for _, nodeServices := range poolServices.NodesExt {
		var queryServices services
		for name, protocol := range n1qlProtocols {
			if nodeServices.Services[name] != 0 {
				if queryServices == nil {
					queryServices = services{}
				}
				queryServices[protocol] = nodeServices.Services[name]
			}
		}

		if len(queryServices) == 0 { // no n1ql service at this node
			continue
		}

		hostname := nodeServices.Hostname

		// nodeServices.Hostname is either a fully-qualified domain name or
		// the empty string - which indicates 127.0.0.1
		// For constructing URLs with raw IPv6 addresses- the IPv6 address
		// must be enclosed within ‘[‘ and ‘]’ brackets.
		if hostname == "" {
			hostname = server.GetIP(true)
		}

		mgmtPort := nodeServices.Services["mgmt"]
		if mgmtPort == 0 {

			// shouldn't happen; all nodes should have a mgmt port
			// should probably log a warning and this node gets ignored
			// TODO: log warning (when signature has warnings)
			continue
		}

		// Query nodes are unique per mgmt endpoint - which means they are unique
		// per Couchbase Server node instance - so we give query nodes an ID
		// which reflects that. Note that in particular query nodes are not
		// guaranteed to be unique per host.
		nodeId := hostname + ":" + strconv.Itoa(mgmtPort)

		queryNodeNames = append(queryNodeNames, nodeId)
		queryNodes[nodeId] = queryServices
	}

	this.Lock()
	defer this.Unlock()
	this.queryNodeNames = queryNodeNames
	this.queryNodes = queryNodes

	this.poolSrvRev = poolServices.Rev
	return this.queryNodeNames, nil
}

func (this *cbCluster) QueryNodeByName(name string) (clustering.QueryNode, errors.Error) {
	qryNodeNames, err := this.QueryNodeNames()
	if err != nil {
		return nil, err
	}
	qryNodeName := ""
	for _, q := range qryNodeNames {
		if name == q {
			qryNodeName = q
			break
		}
	}
	if qryNodeName == "" {
		return nil, errors.NewAdminNoNodeError(name)
	}

	qryNode := &cbQueryNodeConfig{
		ClusterName:   this.Name(),
		QueryNodeName: qryNodeName,
	}

	// We find the host based on query name
	queryHost, _ := server.HostNameandPort(qryNodeName)

	// Since we are using it in the URL
	if strings.Contains(queryHost, ":") {
		queryHost = "[" + queryHost + "]"
	}

	for protocol, port := range this.queryNodes[qryNodeName] {
		switch protocol {
		case _HTTP:
			qryNode.Query = makeURL(protocol, queryHost, port, http.ServicePrefix())
			qryNode.Admin = makeURL(protocol, queryHost, port, http.AdminPrefix())
		case _HTTPS:
			qryNode.QuerySSL = makeURL(protocol, queryHost, port, http.ServicePrefix())
			qryNode.AdminSSL = makeURL(protocol, queryHost, port, http.AdminPrefix())
		}
	}

	return qryNode, nil
}

func (this *cbCluster) Datastore() datastore.Datastore {
	return this.dataStore
}

func (this *cbCluster) AccountingStore() accounting.AccountingStore {
	return this.acctStore
}

func (this *cbCluster) ConfigurationStore() clustering.ConfigurationStore {
	return this.configStore
}

func (this *cbCluster) Version() clustering.Version {
	if this.version == nil {
		this.version = clustering.NewVersion(this.VersionString)
	}
	return this.version
}

func (this *cbCluster) ClusterManager() clustering.ClusterManager {
	return this
}

// cbCluster implements clustering.ClusterManager interface
func (this *cbCluster) Cluster() clustering.Cluster {
	return this
}

func (this *cbCluster) AddQueryNode(n clustering.QueryNode) (clustering.QueryNode, errors.Error) {
	// NOP. This is a read-only implementation
	return nil, nil
}

func (this *cbCluster) RemoveQueryNode(n clustering.QueryNode) (clustering.QueryNode, errors.Error) {
	return this.RemoveQueryNodeByName(n.Name())
}

func (this *cbCluster) RemoveQueryNodeByName(name string) (clustering.QueryNode, errors.Error) {
	// NOP. This is a read-only implementation
	return nil, nil
}

func (this *cbCluster) GetQueryNodes() ([]clustering.QueryNode, errors.Error) {
	qryNodes := []clustering.QueryNode{}
	names, err := this.QueryNodeNames()
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		qryNode, err := this.QueryNodeByName(name)
		if err != nil {
			return nil, err
		}
		qryNodes = append(qryNodes, qryNode)
	}
	return qryNodes, nil
}

// cbQueryNodeConfig implements clustering.QueryNode
type cbQueryNodeConfig struct {
	ClusterName   string                    `json:"cluster"`
	QueryNodeName string                    `json:"name"`
	Query         string                    `json:"queryEndpoint,omitempty"`
	Admin         string                    `json:"adminEndpoint,omitempty"`
	QuerySSL      string                    `json:"querySecure,omitempty"`
	AdminSSL      string                    `json:"adminSecure,omitempty"`
	ClusterRef    *cbCluster                `json:"-"`
	StandaloneRef *clustering.StdStandalone `json:"-"`
	OptionsCL     *clustering.ClOptions     `json:"options"`
}

// cbQueryNodeConfig implements Stringer interface
func (this *cbQueryNodeConfig) String() string {
	return getJsonString(this)
}

// cbQueryNodeConfig implements clustering.QueryNode interface
func (this *cbQueryNodeConfig) Cluster() clustering.Cluster {
	return this.ClusterRef
}

func (this *cbQueryNodeConfig) Name() string {
	return this.QueryNodeName
}

func (this *cbQueryNodeConfig) QueryEndpoint() string {
	return this.Query
}

func (this *cbQueryNodeConfig) ClusterEndpoint() string {
	return this.Admin
}

func (this *cbQueryNodeConfig) QuerySecure() string {
	return this.QuerySSL
}

func (this *cbQueryNodeConfig) ClusterSecure() string {
	return this.AdminSSL
}

func (this *cbQueryNodeConfig) Standalone() clustering.Standalone {
	return this.StandaloneRef
}

func (this *cbQueryNodeConfig) Options() clustering.QueryNodeOptions {
	return this.OptionsCL
}

func getJsonString(i interface{}) string {
	serialized, _ := json.Marshal(i)
	s := bytes.NewBuffer(append(serialized, '\n'))
	return s.String()
}

// ns_server shutdown protocol: poll stdin and exit upon reciept of EOF
func Enable_ns_server_shutdown() {
	go pollStdin()
}

func pollStdin() {
	reader := bufio.NewReader(os.Stdin)
	logging.Infop("pollEOF: About to start stdin polling")
	for {
		ch, err := reader.ReadByte()
		if err == io.EOF {
			logging.Infop("Received EOF; Exiting...")
			os.Exit(0)
		}
		if err != nil {
			logging.Errorp("Unexpected error polling stdin",
				logging.Pair{"error", err})
			os.Exit(1)
		}
		if ch == '\n' || ch == '\r' {
			logging.Infop("Received EOL; Exiting...")
			// TODO: "graceful" shutdown should be placed here
			os.Exit(0)
		}
	}
}

func makeURL(protocol string, host string, port int, endpoint string) string {
	if port == 0 {
		return ""
	}
	urlParts := []string{protocol, "://", host, ":", strconv.Itoa(port), endpoint}
	return strings.Join(urlParts, "")
}
