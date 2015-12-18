(ns global-windows.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx.bookkeeper/server? true
   :onyx.bookkeeper/local-quorum? true
   :onyx.bookkeeper/local-quorum-ports [3196 3197 3198]
   :onyx/id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx/id id
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def batch-size 10)

(def workflow
  [[:in :identity]
   [:identity :out]
   [:agg-in :multiply]
   [:multiply :final-out]
   ])

(defn multiply [segment]
  (update-in segment [:n] * 1000))

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :identity
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    ;:onyx/uniqueness-key :n
    :onyx/batch-size batch-size}

   {:onyx/name :multiply
    :onyx/fn ::multiply
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def capacity 1000)

(def input-chan (chan capacity))

(def agg-input-chan (chan capacity))

(def output-chan (chan capacity))

(def final-output-chan (chan capacity))

(def input-segments
  [{:n 0 :color :red    :event-time #inst "2015-09-13T03:00:00.829-00:00"}
   {:n 1 :color :red    :event-time #inst "2015-09-13T03:03:00.829-00:00"}
   {:n 2 :color :blue   :event-time #inst "2015-09-13T03:07:00.829-00:00"}
   {:n 3 :color :blue   :event-time #inst "2015-09-13T03:11:00.829-00:00"}
   {:n 4 :color :blue   :event-time #inst "2015-09-13T03:15:00.829-00:00"}
   {:n 5 :color :orange :event-time #inst "2015-09-13T03:02:00.829-00:00"}
   {:n 6 :color :blue   :event-time #inst "2015-09-13T03:08:00.829-00:00"}
   {:n 7 :color :blue   :event-time #inst "2015-09-13T03:14:00.829-00:00"}
   {:n 8 :color :blue   :event-time #inst "2015-09-13T03:15:00.829-00:00"}
   {:n 9 :color :orange :event-time #inst "2015-09-13T03:32:00.829-00:00"}
   ])

(doseq [segment input-segments]
  (>!! input-chan segment))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-peers (count (set (mapcat identity workflow))))

(def v-peers (onyx.api/start-peers n-peers peer-group))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan input-chan})

(defn inject-agg-in-ch [event lifecycle]
  {:core.async/chan agg-input-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan output-chan})

(defn inject-final-out-ch [event lifecycle]
  {:core.async/chan final-output-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def agg-in-calls
  {:lifecycle/before-task-start inject-agg-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def final-out-calls
  {:lifecycle/before-task-start inject-final-out-ch})

(def lifecycles
  [
   {:lifecycle/task :in
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}
   {:lifecycle/task :agg-in
    :lifecycle/calls ::agg-in-calls}
   {:lifecycle/task :agg-in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :final-out
    :lifecycle/calls ::final-out-calls}
   {:lifecycle/task :final-out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}
   ])

(def sum
  {:aggregation/init               (fn [window] {})
   :aggregation/fn                 (fn [state window segment]
                                     (let [k (second (:window/aggregation window))
                                           level (:onyx/group-by-key window)]
                                       [:set-value (update-in state
                                                              [(get segment level)]
                                                              (fn [current-value]
                                                                (+ (or current-value 0)
                                                                   (get segment k))))]))
   :aggregation/apply-state-update (fn [state [changelog-type value]]
                                     (case changelog-type
                                       :set-value value))})

(def windows
  [{:window/id :collect-segments
    :window/task :identity
    :window/type :global
    :onyx/group-by-key :color
    :window/aggregation ::sum
    :window/window-key :event-time
    }])

(defn done? [event window-id lower upper segment]
  (= :done segment))

(def triggers
  [{:trigger/window-id :collect-segments
    :trigger/refinement :accumulating
    :trigger/on :punctuation
    :trigger/pred ::done?
    ;:trigger/threshold [5 :elements]
    :trigger/sync ::queue-aggregates!}
   ])

(defn dump-window! [event window-id lower-bound upper-bound state]
  (println (format "Window extent %s, [%s - %s] contents: %s"
                   window-id lower-bound upper-bound state)))

(defn queue-aggregates! [event window-id lower-bound upper-bound state]
  (doseq [[k v] state]
    (>!! agg-input-chan {:color k :n v}))
  (>!! agg-input-chan :done)
  (close! agg-input-chan))

(onyx.api/submit-job
 peer-config
 {:workflow workflow
  :catalog catalog
  :lifecycles lifecycles
  :windows windows
  :triggers triggers
  :task-scheduler :onyx.task-scheduler/balanced})

;; Sleep until the trigger timer fires.
(Thread/sleep 5000)

(>!! input-chan :done)

(close! input-chan)

(def results (take-segments! output-chan))
(clojure.pprint/pprint "output-chan results:" results)
(def final-results (take-segments! final-output-chan))
(clojure.pprint/pprint "final-output-chan results:" final-results)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
