
//
// Definition for Cassandra loader
//

// Import KSonnet library.
local k = import "ksonnet.beta.2/k.libsonnet";

// Short-cuts to various objects in the KSonnet library.
local depl = k.extensions.v1beta1.deployment;
local container = depl.mixin.spec.template.spec.containersType;
local resources = container.resourcesType;
local env = container.envType;
local base = import "base.jsonnet";
local annotations = depl.mixin.spec.template.metadata.annotations;

local cassandra(input, output) =
    local worker(config) = {

        local version = import "version.jsonnet",

        name: "analytics-cassandra",
        images: ["gcr.io/trust-networks/analytics-cassandra:" + version],

        input: input,
        output: output,
        
        // Environment variables
        envs:: [

            // Hostname of Cherami
            env.new("CHERAMI_FRONTEND_HOST", "cherami"),

            // Cassandra settings.
            env.new("CASSANDRA_KEYSPACE", "rdf"),
            env.new("CASSANDRA_CONTACTS", "cassandra")

        ],

        // Container definition.
        containers:: [

            container.new(self.name, self.images[0]) +
                container.env(self.envs) +
                container.args(["/queue/" + input] +
                               std.map(function(x) "output:/queue/" + x,
                                       output)) +
                container.mixin.resources.limits({
                    memory: "128M", cpu: "1.0"
                }) +
                container.mixin.resources.requests({
                    memory: "128M", cpu: "0.2"
                })

        ],

        // Deployment definition.  replicas is number of container replicas,
        // inp is the input queue name, out is an array of output queue names.
        deployments:: [
            depl.new(self.name,
                     config.workers.replicas.cassandra,
                     self.containers,
                     {app: "analytics-cassandra",
                      component: "analytics"}) +
	    annotations({"prometheus.io/scrape": "true", 
			 "prometheus.io/port": "8080"})
        ],

        resources: self.deployments

    };
    worker;

cassandra

