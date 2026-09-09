(ns jepsen.multiraft.nemesis
  "Local process kill/restart nemesis (no SSH).

  Reads $DATA/node-$id.pid (or :data-dir from test), kill -9, then restarts
  multiraft-demo via scripts/start_one_node.sh (shared with chaos/acceptance)."
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen
             [generator :as gen]
             [nemesis :as nemesis]]
            [jepsen.multiraft.cluster :as cluster])
  (:import (java.io File)))

(defn env-or
  [k default]
  (or (System/getenv k) default))

(defn data-dir
  [test]
  (cluster/data-dir test))

(defn demo-bin
  [test]
  (or (:demo-bin test)
      (System/getenv "DEMO_BIN")
      (str (env-or "MULTIRAFT_ROOT" ".") "/target/debug/multiraft-demo")))

(defn base-port
  [test]
  (cluster/base-port test))

(defn groups
  [test]
  (or (:groups test)
      (some-> (System/getenv "GROUPS") Integer/parseInt)
      1))

(defn node-count
  [test]
  (or (:node-count test)
      (count (:nodes test))
      3))

(defn- pid-file
  [test id]
  (str (data-dir test) "/node-" id ".pid"))

(defn- read-pid
  [test id]
  (let [f (pid-file test id)]
    (when (.exists (File. f))
      (let [s (str/trim (slurp f))]
        (when-not (str/blank? s)
          (Long/parseLong s))))))

(defn- pid-alive?
  [pid]
  (when pid
    (zero? (:exit (shell/sh "kill" "-0" (str pid))))))

(defn- voter-ids
  "Nodes that participate in the Raft quorum (exclude Standby extras)."
  [test]
  (mapv str (:nodes test)))

(defn- live-ids
  [test]
  (->> (voter-ids test)
       (filter #(pid-alive? (read-pid test %)))
       vec))

(defn- kill-node!
  [test id]
  (if-let [pid (read-pid test id)]
    (if (pid-alive? pid)
      (do
        (info "nemesis kill -9 node" id "pid" pid)
        (shell/sh "kill" "-9" (str pid))
        (shell/sh "bash" "-c" (str "wait " pid " 2>/dev/null || true"))
        {:killed id :pid pid})
      (do
        (warn "nemesis: pid" pid "for node" id "not alive")
        {:skipped id :reason :not-alive}))
    (do
      (warn "nemesis: missing pid file for node" id)
      {:skipped id :reason :no-pid})))

(defn- start-node!
  [test id]
  (let [root (env-or "MULTIRAFT_ROOT" ".")
        script (str root "/scripts/start_one_node.sh")
        data (data-dir test)]
    (info "nemesis restart voter" id "via" script)
    (try
      (let [result (shell/sh "bash" script (str id) "voter"
                             :dir root
                             :env (merge (into {} (System/getenv))
                                         {"DATA_DIR" data
                                          "DATA" data
                                          "DEMO_BIN" (demo-bin test)
                                          "MULTIRAFT_ROOT" root
                                          "JEPSEN" (or (System/getenv "JEPSEN") "0")
                                          "NO_AUTO_PROPOSE" (or (System/getenv "NO_AUTO_PROPOSE") "1")}))]
        (when (not= 0 (:exit result))
          (warn "nemesis restart script failed" (:err result)))
        (Thread/sleep 500)
        (if-let [pid (read-pid test id)]
          {:started id :pid pid}
          {:started id :error "missing pid after restart"}))
      (catch Throwable t
        (warn "nemesis restart failed" (.getMessage t))
        {:started id :error (.getMessage t)}))))

(defn- pick-kill-target
  "Prefer a random live node, but keep majority alive after the kill."
  [test]
  (let [live (live-ids test)
        majority (inc (quot (node-count test) 2))
        n (count live)]
    (when (and (seq live) (>= (dec n) majority))
      (rand-nth live))))

(defrecord ProcessKiller []
  nemesis/Nemesis
  (setup! [this _test] this)

  (invoke! [this test op]
    (let [value (case (:f op)
                  :kill (if-let [id (or (:node (:value op))
                                        (pick-kill-target test))]
                          (kill-node! test id)
                          {:skipped :no-safe-target :live (live-ids test)})
                  :start (let [down (->> (map str (:nodes test))
                                         (remove #(pid-alive? (read-pid test %)))
                                         vec)]
                           (if (seq down)
                             (start-node! test (rand-nth down))
                             {:skipped :all-up :live (live-ids test)}))
                  op)]
      (assoc op :value value)))

  (teardown! [this _test]
    this))

(defn process-killer
  []
  (ProcessKiller.))

(defn generator
  "Alternating kill / restart with sleeps (for custom wiring)."
  []
  (gen/cycle
   [(gen/sleep 4)
    {:type :info :f :kill}
    (gen/sleep 4)
    {:type :info :f :start}]))
