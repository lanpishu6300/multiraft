(ns jepsen.multiraft.client
  "HTTP client against multiraft-demo admin endpoints."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [jepsen.client :as client]
            [jepsen.multiraft.cluster :as cluster])
  (:import (java.net ConnectException SocketTimeoutException)))

(defn base-port
  "Raft base port from cluster descriptor, test map, or BASE_PORT env."
  [test]
  (cluster/base-port test))

(defn admin-url
  "Admin HTTP for node id string/number."
  [test node]
  (cluster/admin-url test node))

(defn nodes
  [test]
  (cluster/voter-ids test))

(defn- parse-body
  [resp]
  (let [b (:body resp)]
    (cond
      (map? b) b
      (string? b) (json/parse-string b true)
      :else nil)))

(defn- http-opts
  []
  {:socket-timeout 3000
   :connection-timeout 2000
   :throw-exceptions false
   :as :text
   :headers {"Content-Type" "application/json"
             "Accept" "application/json"}})

(defn- connection-error?
  [ex]
  (or (instance? ConnectException ex)
      (instance? SocketTimeoutException ex)
      (when-let [msg (.getMessage ^Throwable ex)]
        (re-find #"(?i)connection refused|timed out|Connection reset" msg))))

(def ^:private idem-counter (atom 0))

(defn- next-client-idem
  "Client-unique idem so retries after failover never collide across nodes."
  []
  (bit-or (bit-shift-left 0xC1E07 40) ; client namespace
          (bit-and (swap! idem-counter inc) 0xffffffffff)))

(defn- try-add!
  "POST /groups/0/inc. Returns [:ok] | [:fail err] | [:info err].

  On success the invoke! handler keeps op `:value` as the add delta for
  `checker/counter` (do not substitute Raft index).

  Uses a fixed `idem` for the whole failover loop so a successful propose
  followed by a retry does not double-apply."
  [url delta idem]
  (try
    (let [resp (http/post (str url "/groups/0/inc")
                          (merge (http-opts)
                                 {:body (json/generate-string
                                         {:delta delta :idem idem})}))
          status (:status resp)
          body (parse-body resp)]
      (cond
        (and (= 200 status) (:ok body))
        [:ok]

        (#{503 409} status)
        [:fail {:status status :error (:error body "not-leader")}]

        :else
        [:fail {:status status :body body}]))
    (catch Throwable t
      (if (connection-error? t)
        [:fail {:error :conn :msg (.getMessage t)}]
        ;; Ambiguous: request may have been accepted.
        [:info {:error :exception :msg (.getMessage t)}]))))

(defn- try-read!
  "GET /groups/0/value; only accept linearizable consistency."
  [url]
  (try
    (let [resp (http/get (str url "/groups/0/value") (http-opts))
          status (:status resp)
          body (parse-body resp)]
      (cond
        (= 503 status)
        [:fail {:status 503 :error :unavailable}]

        (and (= 200 status) (= "linearizable" (:consistency body)))
        [:ok (:value body)]

        (and (= 200 status) (= "local" (:consistency body)))
        [:fail {:error :stale :value (:value body)}]

        ;; Standby service offload — never feed into checker/counter.
        (and (= 200 status) (= "stale" (:consistency body)))
        [:fail {:error :stale-offload :value (:value body)}]

        :else
        [:fail {:status status :body body}]))
    (catch Throwable t
      (if (connection-error? t)
        [:fail {:error :conn :msg (.getMessage t)}]
        [:info {:error :exception :msg (.getMessage t)}]))))

(defn- with-node-failover
  "Try preferred node, then other nodes. op-fn returns [:ok ...] | [:fail e] | [:info e]."
  [test preferred op-fn]
  (let [order (cons preferred (remove #{preferred} (nodes test)))]
    (loop [ns order
           last-fail nil]
      (if (empty? ns)
        [:fail (or last-fail {:error :all-nodes-failed})]
        (let [url (admin-url test (first ns))
              res (op-fn url)
              tag (first res)]
          (case tag
            :ok res
            :info res
            :fail (recur (rest ns) (second res))))))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn {:node node :test test}))

  (setup! [this _test] this)

  (invoke! [this test op]
    (let [node (or (:node (:conn this)) (first (nodes test)))
          ;; One idem per invoke so NotLeader retries are safe (dedupe).
          add-idem (when (= :add (:f op)) (next-client-idem))
          res (case (:f op)
                :add (with-node-failover test node
                       (fn [url] (try-add! url (or (:value op) 1) add-idem)))
                :read (with-node-failover test node
                        try-read!)
                (throw (ex-info "unknown op" {:op op})))
          tag (first res)]
      (case tag
        ;; Keep :add :value as delta for checker/counter.
        :ok (if (= :add (:f op))
              (assoc op :type :ok)
              (assoc op :type :ok :value (second res)))
        :fail (assoc op :type :fail :error (second res))
        :info (assoc op :type :info :error (second res)))))

  (teardown! [this _test] this)

  (close! [_this _test]))

(defn client
  []
  (Client. nil))
