package main

// Cassandra loader for the analytics cluster.  Takes events on input queue
// and restructures for loading into a set of Cassandra tables describing an
// RDF graph.  Multiple RDF statements are loaded per event.

// No output queues are used.

// FIXME: This currently does nothing.  Doing stuff code is commented out,
// wasn't ported to new JSON model.

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	dt "github.com/trustnetworks/analytics-common/datatypes"
	"github.com/trustnetworks/analytics-common/utils"
	"github.com/trustnetworks/analytics-common/worker"
	"github.com/gocql/gocql"
)

const pgm = "cassandra"

type work struct {
	config   *gocql.ClusterConfig
	sess     *gocql.Session
	keyspace string
	contacts string
}

type triple struct {
	s, p, o string
}

var (
	cybobj  string = "http://cyberprobe.sf.net/obj/"
	cybprop string = "http://cyberprobe.sf.net/prop/"
	cybtype string = "http://cyberprobe.sf.net/type/"
	rdf     string = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	rdfs    string = "http://www.w3.org/2000/01/rdf-schema#"

	rdftype   string = rdf + "type"
	rdfslabel string = rdfs + "label"
)

func obj_uri(tp string, id string) string {
	return cybobj + tp + "/" + id
}

func prop_uri(prop string) string {
	return cybprop + prop
}

func type_uri(t string) string {
	return cybtype + t
}

func (w *work) add_string(obs *[]triple, s, p, o string) {
	*obs = append(*obs, triple{"u:" + s, "u:" + p, "s:" + o})
}

func (w *work) add_uri(obs *[]triple, s, p, o string) {
	*obs = append(*obs, triple{"u:" + s, "u:" + p, "u:" + o})
}

func (w *work) add_dt(obs *[]triple, s, p, o string) {
	*obs = append(*obs, triple{"u:" + s, "u:" + p, "d:" + o})
}

func (w *work) add_int(obs *[]triple, s, p string, o int) {
	*obs = append(*obs, triple{"u:" + s, "u:" + p, "s:" + string(o)})
}

func (s *work) init() error {

	s.keyspace = utils.Getenv("CASSANDRA_KEYSPACE", "rdf")
	s.contacts = utils.Getenv("CASSANDA_CONTACTS", "cassandra")

	utils.Log("Initialising...")
	s.config = gocql.NewCluster()
	s.config.Hosts = strings.Split(s.contacts, ",")

	var err error
	for {
		s.sess, err = s.config.CreateSession()
		if err == nil {
			break
		}

		utils.Log("Could not create session: %s", err.Error())
		time.Sleep(time.Second * 5)
	}

	utils.Log("Connected.")

	utils.Log("Create keyspace '%s'...", s.keyspace)
	// Ignore error.
	err = s.sess.Query(`
           CREATE KEYSPACE ` + s.keyspace + ` WITH REPLICATION = {
               'class': 'SimpleStrategy', 'replication_factor': '3'
           }`).Exec()
	if err != nil {
		utils.Log("keyspace error (ignored): %s", err.Error())
	}

	s.config.Keyspace = "rdf"

	for {
		s.sess, err = s.config.CreateSession()
		if err == nil {
			break
		}

		utils.Log("Could not create session: %s", err.Error())
		time.Sleep(time.Second * 5)
	}

	utils.Log("Connected to keyspace.")

	err = s.sess.Query(`
            CREATE TABLE spo (s text, p text, o text, primary key (s, p, o))
        `).Exec()
	if err != nil {
		utils.Log("Create error (ignored): %s", err.Error())
	}

	err = s.sess.Query(`
            CREATE TABLE pos (s text, p text, o text, primary key (p, o, s))
        `).Exec()
	if err != nil {
		utils.Log("Create error (ignored): %s", err.Error())
	}

	err = s.sess.Query(`
            CREATE TABLE osp (s text, p text, o text, primary key (o, s, p))
        `).Exec()
	if err != nil {
		utils.Log("Create error (ignored): %s", err.Error())
	}

	obs := make([]triple, 0, 0)

	// Observation type
	s.add_uri(&obs, type_uri("observation"), rdftype, rdf+"Resource")
	s.add_string(&obs, type_uri("observation"), rdfslabel, "Observation")

	// Device type
	s.add_uri(&obs, type_uri("device"), rdftype, rdf+"Resource")
	s.add_string(&obs, type_uri("device"), rdfslabel, "Device")

	// Device property on Observation.
	s.add_uri(&obs, prop_uri("device"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("device"), rdfslabel, "Device")

	// Method property on Observation.
	s.add_uri(&obs, prop_uri("method"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("method"), rdfslabel, "Method")

	// Action property on Observation.
	s.add_uri(&obs, prop_uri("action"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("action"), rdfslabel, "Action")

	// Code property on Observation.
	s.add_uri(&obs, prop_uri("code"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("code"), rdfslabel, "Code")

	// Command property on Observation.
	s.add_uri(&obs, prop_uri("command"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("command"), rdfslabel, "Command")

	// Status property on Observation.
	s.add_uri(&obs, prop_uri("status"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("status"), rdfslabel, "Status")

	// URL property on Observation.
	s.add_uri(&obs, prop_uri("url"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("url"), rdfslabel, "URL")

	// Time property on Observation.
	s.add_uri(&obs, prop_uri("time"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("time"), rdfslabel, "Time")

	// Country property on Observation.
	s.add_uri(&obs, prop_uri("country"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("country"), rdfslabel, "Country of origin")

	// Message type property on Observation
	s.add_uri(&obs, prop_uri("type"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("type"), rdfslabel, "Type")

	// DNS Query on Observation
	s.add_uri(&obs, prop_uri("query"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("query"), rdfslabel, "DNS Query")

	// DNS Answer (name) on Observation
	s.add_uri(&obs, prop_uri("answer_name"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("answer_name"), rdfslabel, "Answer (name)")

	// DNS Query on Observation
	s.add_uri(&obs, prop_uri("answer_address"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("answer_address"), rdfslabel,
		"Answer (address)")

	// Protocol context
	s.add_uri(&obs, prop_uri("context"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("context"), rdfslabel, "Context")

	// From property on Observation
	s.add_uri(&obs, prop_uri("from"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("from"), rdfslabel, "From")

	// To property on Observation
	s.add_uri(&obs, prop_uri("to"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("to"), rdfslabel, "To")

	// Source property on Observation
	s.add_uri(&obs, prop_uri("source"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("source"), rdfslabel, "Source address")

	// Destination property on Observation
	s.add_uri(&obs, prop_uri("destination"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("destination"), rdfslabel,
		"Destination address")

	// IP type
	s.add_uri(&obs, type_uri("ip"), rdftype, rdf+"Resource")
	s.add_string(&obs, type_uri("ip"), rdfslabel, "IP address")

	// TCP type
	s.add_uri(&obs, type_uri("tcp"), rdftype, rdf+"Resource")
	s.add_string(&obs, type_uri("tcp"), rdfslabel, "TCP port")

	// UDP type
	s.add_uri(&obs, type_uri("udp"), rdftype, rdf+"Resource")
	s.add_string(&obs, type_uri("udp"), rdfslabel, "UDP port")

	// Port property on TCP and UDP
	s.add_uri(&obs, prop_uri("port"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("port"), rdfslabel, "Port")

	// IP property on IP, TCP and UDP
	s.add_uri(&obs, prop_uri("ip"), rdftype, rdf+"Property")
	s.add_string(&obs, prop_uri("ip"), rdfslabel, "IP")

	s.output(&obs)

	return nil

}

func (s *work) output(obs *[]triple) error {

	batch := s.sess.NewBatch(gocql.LoggedBatch)

	for _, o := range *obs {

		batch.Query("INSERT INTO spo (s, p, o) VALUES (?, ?, ?)",
			o.s, o.p, o.o)

		batch.Query("INSERT INTO pos (s, p, o) VALUES (?, ?, ?)",
			o.s, o.p, o.o)

		batch.Query("INSERT INTO osp (s, p, o) VALUES (?, ?, ?)",
			o.s, o.p, o.o)

	}

	err := s.sess.ExecuteBatch(batch)

	if err != nil {
		utils.Log("Exception: %s", err.Error())
	}

	// FIXME: Ignore errors for now.

	return nil

}

func (h *work) Handle(msg []uint8, w *worker.Worker) error {

	var event dt.Event

	err := json.Unmarshal(msg, &event)
	if err != nil {
		utils.Log("Couldn't unmarshal json: %s", err.Error())
		return nil
	}

	if event.Action == "connected_up" {
		return nil
	}
	if event.Action == "connected_down" {
		return nil
	}

	var obs []triple
	obs = make([]triple, 0)

	id := event.Id

	uri := obj_uri("obs", id)
	h.add_string(&obs, uri, rdftype, type_uri("observation"))

	switch event.Action {

	case "unrecognised_datagram":
		h.add_string(&obs, uri, rdfslabel, "unrecognised datagram")

	case "unrecognised_stream":
		h.add_string(&obs, uri, rdfslabel, "unrecognised stream")

	case "icmp":
		h.add_string(&obs, uri, rdfslabel, "ICMP")

	case "http_request":
		h.add_string(&obs, uri, rdfslabel,
			"HTTP "+event.HttpRequest.Method+" "+event.Url)
		h.add_string(&obs, uri, prop_uri("method"),
			event.HttpRequest.Method)
		for k, v := range event.HttpRequest.Header {
			h.add_string(&obs, uri, prop_uri("header:"+k), v)
		}

	case "http_response":
		h.add_string(&obs, uri, rdfslabel,
			"HTTP "+strconv.Itoa(event.HttpResponse.Code)+" "+
				event.HttpResponse.Status+" "+event.Url)
		h.add_string(&obs, uri, prop_uri("status"),
			event.HttpResponse.Status)
		for k, v := range event.HttpResponse.Header {
			h.add_string(&obs, uri, prop_uri("header:"+k), v)
		}

	case "dns_message":
		var lbl string
		if event.DnsMessage.Type == "query" {
			lbl = "DNS query"
		} else {
			lbl = "DNS answer"
		}
		for _, v := range event.DnsMessage.Query {
			lbl = lbl + " " + v.Name
		}
		h.add_string(&obs, uri, rdfslabel, lbl)

		h.add_string(&obs, uri, prop_uri("dnsType"),
			event.DnsMessage.Type)

		if event.DnsMessage.Query != nil {
			if len(event.DnsMessage.Query) > 0 {
				for _, v := range event.DnsMessage.Query {
					h.add_string(&obs, uri,
						prop_uri("query"),
						v.Name)
				}
			}
		}

		if event.DnsMessage.Answer != nil {
			if len(event.DnsMessage.Answer) > 0 {
				for _, v := range event.DnsMessage.Answer {
					if v.Name != "" {
						h.add_string(&obs, uri,
							prop_uri("answer_name"),
							v.Name)
					}
					if v.Address != "" {
						h.add_string(&obs,
							uri,
							prop_uri("answer_address"),
							v.Address)
					}
				}
			}
		}

	case "ftp_command":
		h.add_string(&obs, uri, rdfslabel,
			"FTP "+event.FtpCommand.Command)
		h.add_string(&obs, uri, prop_uri("command"),
			event.FtpCommand.Command)

	case "ftp_response":
		h.add_string(&obs, uri, rdfslabel,
			"FTP "+string(event.FtpResponse.Status))
		h.add_int(&obs, uri, prop_uri("status"),
			event.FtpResponse.Status)

		if len(event.FtpResponse.Text) > 0 {
			t := ""
			s := ""
			for _, v := range event.FtpResponse.Text {
				t = t + v + s
				s = " "
			}
			h.add_string(&obs, uri, prop_uri("text"), t)
		}

	case "smtp_command":
		h.add_string(&obs, uri, rdfslabel,
			"SMTP "+string(event.SmtpCommand.Command))
		h.add_string(&obs, uri, prop_uri("command"),
			event.SmtpCommand.Command)
	case "smtp_response":
		h.add_string(&obs, uri, rdfslabel,
			"SMTP "+string(event.SmtpResponse.Status))
		h.add_int(&obs, uri, prop_uri("status"),
			event.SmtpResponse.Status)

		if len(event.FtpResponse.Text) > 0 {
			t := ""
			s := ""
			for _, v := range event.SmtpResponse.Text {
				t = t + v + s
				s = " "
			}
			h.add_string(&obs, uri, prop_uri("text"), t)
		}

	case "smtp_data":
		var lbl string
		lbl = "SMTP " + event.SmtpData.From
		for _, v := range event.SmtpData.To {
			lbl = lbl + " " + v
		}
		h.add_string(&obs, uri, rdfslabel, lbl)

		if event.SmtpData.From != "" {
			h.add_string(&obs, uri, prop_uri("from"),
				event.SmtpData.From)
		}

		if len(event.SmtpData.To) > 0 {
			for _, v := range event.SmtpData.To {
				h.add_string(&obs, uri, prop_uri("to"), v)
			}
		}

	}

	h.add_string(&obs, uri, prop_uri("action"), event.Action)
	h.add_uri(&obs, uri, prop_uri("device"),
		obj_uri("device", event.Device))

	h.add_uri(&obs, obj_uri("device", event.Device), rdftype,
		type_uri("device"))
	h.add_string(&obs, obj_uri("device", event.Device), rdfslabel,
		"Device "+event.Device)

	h.add_dt(&obs, uri, prop_uri("time"), event.Time)

	if event.Url != "" {
		h.add_uri(&obs, uri, prop_uri("url"), event.Url)
	}

	if len(event.Src) > 0 {

		var ip string

		for _, v := range event.Src {

			var cls, addr string

			val_parts := strings.SplitN(v, ":", 2)
			cls = val_parts[0]
			if len(val_parts) > 1 {
				addr = val_parts[1]
			} else {
				addr = ""
			}

			if cls == "tcp" {

				fulladdr := ip + ":" + addr

				h.add_uri(&obs, uri, prop_uri("src"),
					obj_uri("tcp", fulladdr))
				h.add_uri(&obs, obj_uri("tcp", fulladdr),
					rdftype,
					type_uri("tcp"))
				h.add_string(&obs, obj_uri("tcp", fulladdr),
					rdfslabel,
					"TCP "+fulladdr)
				h.add_uri(&obs, obj_uri("tcp", fulladdr),
					prop_uri("context"),
					obj_uri("ip", ip))
				h.add_string(&obs, obj_uri("tcp", fulladdr),
					prop_uri("ip"),
					ip)
				h.add_string(&obs, obj_uri("tcp", fulladdr),
					prop_uri("port"), addr)

			}

			if cls == "udp" {

				fulladdr := ip + ":" + addr

				h.add_uri(&obs, uri, prop_uri("src"),
					obj_uri("udp", fulladdr))
				h.add_uri(&obs, obj_uri("udp", fulladdr),
					rdftype,
					type_uri("udp"))
				h.add_string(&obs, obj_uri("udp", fulladdr),
					rdfslabel,
					"UDP "+fulladdr)
				h.add_uri(&obs, obj_uri("udp", fulladdr),
					prop_uri("context"),
					obj_uri("ip", ip))
				h.add_string(&obs, obj_uri("udp", fulladdr),
					prop_uri("ip"), ip)
				h.add_string(&obs, obj_uri("udp", fulladdr),
					prop_uri("port"), addr)
			}

			if cls == "ipv4" {

				ip = addr

				h.add_uri(&obs, obj_uri("ip", addr), rdftype,
					type_uri("ip"))
				h.add_string(&obs, obj_uri("ip", addr),
					rdfslabel, addr)
				h.add_string(&obs, obj_uri("ip", addr),
					prop_uri("ip"), addr)

			}

		}

	}

	if len(event.Dest) > 0 {

		var ip string

		for _, v := range event.Dest {

			var cls, addr string

			val_parts := strings.SplitN(v, ":", 2)
			cls = val_parts[0]
			if len(val_parts) > 1 {
				addr = val_parts[1]
			} else {
				addr = ""
			}

			if cls == "tcp" {

				fulladdr := ip + ":" + addr
				h.add_uri(&obs, uri, prop_uri("dest"),
					obj_uri("tcp", fulladdr))
				h.add_uri(&obs, obj_uri("tcp", fulladdr),
					rdftype, type_uri("tcp"))
				h.add_string(&obs, obj_uri("tcp", fulladdr),
					rdfslabel,
					"TCP "+fulladdr)
				h.add_uri(&obs, obj_uri("tcp", fulladdr),
					prop_uri("context"),
					obj_uri("ip", ip))
				h.add_string(&obs, obj_uri("tcp", fulladdr),
					prop_uri("ip"), ip)
				h.add_string(&obs, obj_uri("tcp", fulladdr),
					prop_uri("port"),
					addr)
			}

			if cls == "udp" {

				fulladdr := ip + ":" + addr
				h.add_uri(&obs, uri, prop_uri("dest"),
					obj_uri("udp", fulladdr))
				h.add_uri(&obs, obj_uri("udp", fulladdr),
					rdftype, type_uri("udp"))
				h.add_string(&obs, obj_uri("udp", fulladdr),
					rdfslabel,
					"UDP "+fulladdr)
				h.add_uri(&obs, obj_uri("udp", fulladdr),
					prop_uri("context"),
					obj_uri("ip", ip))
				h.add_string(&obs, obj_uri("udp", fulladdr),
					prop_uri("ip"), ip)
				h.add_string(&obs, obj_uri("udp", fulladdr),
					prop_uri("port"),
					addr)

			}

			if cls == "ipv4" {
				ip = addr
				h.add_uri(&obs, obj_uri("ip", addr), rdftype,
					type_uri("ip"))
				h.add_string(&obs, obj_uri("ip", addr),
					rdfslabel, addr)
				h.add_string(&obs, obj_uri("ip", addr),
					prop_uri("ip"), addr)

			}

		}

	}

	h.output(&obs)

	return nil

}

func main() {

	var w worker.QueueWorker
	var s work
	utils.LogPgm = pgm

	// Initialise.
	err := s.init()
	if err != nil {
		utils.Log("init: %s", err.Error())
		return
	}

	// Initialise
	var input string
	var output []string

	if len(os.Args) > 0 {
		input = os.Args[1]
	}
	if len(os.Args) > 2 {
		output = os.Args[2:]
	}

	err = w.Initialise(input, output, pgm)
	if err != nil {
		utils.Log("init: %s", err.Error())
		return
	}

	utils.Log("Initialisation complete.")

	// Invoke Wye event handling.
	w.Run(&s)

}
