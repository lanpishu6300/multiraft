(ns jepsen.multiraft.cluster
  "Read cluster.json written by scripts/run_demo_cluster.sh."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]))

(defn data-dir
  [test]
  (or (:data-dir test)
      (System/getenv "DATA_DIR")
      (System/getenv "DATA")
      ".jepsen-data"))

(defn descriptor-path
  [test]
  (str (data-dir test) "/cluster.json"))

(defn load-descriptor
  [test]
  (let [f (io/file (descriptor-path test))]
    (when (.exists f)
      (json/parse-string (slurp f) true))))

(defn base-port
  [test]
  (or (:base-port test)
      (:base_port (load-descriptor test))
      (some-> (System/getenv "BASE_PORT") Integer/parseInt)
      23000))

(defn admin-url
  "Admin HTTP for node id; prefers cluster.json admin_url."
  [test node]
  (let [id (if (string? node) (Integer/parseInt node) (int node))
        desc (load-descriptor test)
        from-desc (some #(when (= (:id %) id) (:admin_url %))
                        (:nodes desc))]
    (or from-desc
        (let [port (+ (base-port test) 100 id -1)]
          (str "http://127.0.0.1:" port)))))

(defn voter-ids
  [test]
  (let [desc (load-descriptor test)
        from-desc (when desc
                    (->> (:nodes desc)
                         (filter #(= "voter" (:role %)))
                         (mapv #(str (:id %)))))]
    (or (not-empty from-desc)
        (:nodes test)
        ["1" "2" "3"])))
