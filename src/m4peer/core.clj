;;defines a simple entry point for establishing
;;marathon peers over hazelcast using the
;;api from hazeldemo.
(ns m4peer.core
  (:require [chazel.core :as ch]
            [marathon.analysis.random :as random]
            [marathon.analysis.util :as util]
            [marathon.analysis :as a]
            [hazeldemo.client :as client]
            #_[hazeldemo.worker :as w]
            [hazeldemo.core :as core]
            [chazel.core :as ch]
            [m4peer.patch]
            [clojure.core.async :as async :refer
             [>! <! >!! <!! put! take! chan]]))

;;we eschew this.
#_
(def +workers+ (marathon.analysis.util/guess-physical-cores))
;;need a worker.
#_
(def work-pool
  (do (when (seq @w/workers)
        (println [:reloading-peer])
        (w/kill-all!))
      (w/spawn-workers! +workers+)))

(def ^:dynamic *run-site* :local)

;;force us to reload nippy at runtime since we've extended protocols...
;;runtime serialization patches.
(require 'marathon.serial :reload)
(taoensso.nippy/swap-serializable-whitelist! (fn [old] (conj old "java.util.Random")))

(defn exec-experiments [xs]
  (case *run-site*
    :local   (marathon.analysis.random/parallel-exec xs)
    :cluster (client/fmap marathon.analysis.random/supply-experiment xs) ;;naive
    (throw (ex-info "unknown *run-site*" {:in *run-site*}))))

;;we hook the default, when using the peer, to use our version of exec-experiments.
(alter-var-root #'marathon.analysis.random/*exec-experiments* (fn [_] exec-experiments))


;;Distributed Logging Infrastructure
;;==================================
;;These need to be part of the shared peer service
;;API.

;;if we're running distributed, need to log distributed.
;;do we hard-code the topic, or create a new name hmm...
;;we need a pre-built logging function (maybe in m4peer, or even hazeldemo)
;;that can push logs to a topic.  probably hazeldemo.
;;then clients will shift to that topic for logging.

(defn get-topic [nm]
  (or (core/get-object nm)
      (ch/hz-reliable-topic nm)))

(def listener-id (atom nil))

(defn stop-listening [topic-name id]
  (let [tp (get-topic topic-name)]
    (ch/remove-message-listener tp id)))

(defn start-listening [topic-name f]
  (let [tp (get-topic topic-name)
        id (do (when  @listener-id
                 (stop-listening topic-name @listener-id))
               (ch/add-message-listener tp f))]
    (reset! listener-id id)))

(defn with-ip [msg]
  (str "<" @core/addr "> " msg))

;;maybe we just define a tap?
;;we don't want to redirect logging over the net if we're
;;running on the caller...for now we do though...
;;these probably need to be in m4peer.

(defn start-logging! [topic-name]
  ;;create a topic
  ;;broadcast to everybody via log-topic that they should start logging,
  ;;or use simple message passing?
  ;;what if we have workers join the cluster?
  ;;maybe we have an atomic value for log-topic....
  (let [tp (get-topic topic-name)
        publish (fn [x] (ch/publish tp (with-ip x)))]
    (reset! util/log-fn publish)
    (with-ip "started-cluster-logging")))

;;do we need to clean up the listener here?
(defn stop-logging! [topic-name]
  (let [tp (core/destroy! topic-name)] ;;idempotent.
    (reset! util/log-fn println)
    (with-ip "stopped-cluster-logging")))

(defn stop-all-logging! [topic-name]
  (hazeldemo.client/eval-all! `(m4peer.core/stop-logging! ~topic-name)))

(defn start-all-logging! [topic-name]
  (hazeldemo.client/eval-all! `(m4peer.core/start-logging! ~topic-name)))

(defn echo [x] (util/log x) x)

(defn invoke-all! [f arg] (ch/ftask (partial f arg) :members :all))

(defn print-or-die [topic-name msg]
  (if (= msg :topic/die)
    (stop-all-logging! topic-name)
    (println msg)))

(defn send-die [topic-name]
  (let [tp (get-topic topic-name)]
    (ch/publish tp :topic/die)))

;;need to tell everyone to start-logging!
;;we could use a topic for this. already have some in core....

;;we can package some form of logging for when we reach the end of
;;the results...
;;append a logging call to end, so remove the listener.

(defmacro with-cluster-logging [topic & body]
  `(let [id# (atom nil)]
     (try (let [~'_   (println [:listening-to ~topic])
                ~'_   (reset! id# (m4peer.core/start-listening ~topic (partial m4peer.core/print-or-die ~topic)))
                ~'_   (println [:start-logging ~topic])
                ~'_   (m4peer.core/start-all-logging! ~topic)
                res#  (do ~@body)
                ]
            res#)
          #_
          (finally (m4peer.core/send-die ~topic)))))

;;from random runs examples
(comment
  ;way to invoke functions
  (def path "~/repos/notional/supplyvariation-testdata.xlsx")
  #_(def proj (a/load-project path))
  (def phases [["comp" 1 821] ["phase-1" 822 967]])

  (def run2
    (binding [random/*noisy* 1.0]
      (random/run "~/repos/notional/supplyvariation-testdata.xlsx"
        :reps 5
        :phases phases
        :lower 0 :upper 1.5
        :compo-lengths random/default-compo-lengths
        :levels 3)))

  (def run3
    (binding [random/*noisy* 1.0
              *run-site* :cluster]
      (random/run "~/repos/notional/supplyvariation-testdata.xlsx"
        :reps 5
        :phases phases
        :lower 0 :upper 1.5
        :compo-lengths random/default-compo-lengths
        :levels 3)))


  (def big-run
    (binding [random/*noisy* 1.0
              *run-site* :cluster]
      (random/run "~/repos/notional/supplyvariation-testdata.xlsx"
        :reps 30
        :phases phases
        :lower 0 :upper 1.5
        :compo-lengths random/default-compo-lengths
        #_#_:levels 3)))

  ;;We can just use run....and pass it high level args to control behavior.
  ;;This run will run 1 rep, degrading supply by 1, from a range of 1.5*initial AC supply,
  ;;to 0 AC, with the default component lengths to distribute random cycles by, using
  ;;a predefined set of phases to report results.
  #_
  (def run1
    (binding [*noisy* 1.0]
      (run "~/repos/notional/supplyvariation-testdata.xlsx"
        :reps 1
        :phases phases
        :lower 0 :upper 1.5
        :compo-lengths default-compo-lengths)))

  ;;This run will run 1 rep, degrading supply by 1, from a range of 1.5*initial AC supply,
  ;;to 0 AC, with the default component lengths to distribute random cycles by, using
  ;;a predefined set of phases to report results.  It will only perform 3 levels of
  ;;supply experiments, distributing the levels evenly across the range [lower*supply upper*supply].
  ;;It will perform 5 replications at each level.  All messages will be logged and
  ;;printed to output cleanly as well.
  #_
  (def run2
    (binding [*noisy* 1.0]
      (run "~/repos/notional/supplyvariation-testdata.xlsx"
        :reps 5
        :phases phases
        :lower 0 :upper 1.5
        :compo-lengths default-compo-lengths
        :levels 3)))

  ;;this will generate a different root seed every time, as opposed
  ;;to the previous 2 examples, which will always use +default-seed+
  ;;or 42, unless supplied.
  #_
  (def random-run-ex
    (binding [*noisy* 1.0]
      (random-run "~/repos/notional/supplyvariation-testdata.xlsx"
        :reps 5
        :phases phases
        :lower 0 :upper 1.5
        :compo-lengths default-compo-lengths
        :levels 3)))

  ;;This will change out the default ac supply reduction experiments.
  ;;We now leverage at most a 65-level NOLH design varying all
  ;;components.  At best, we do a full-factorial design of < 65.
  #_
  (def random-run-nolh
    (binding [*noisy* 1.0
              *project->experiments* project->nolh-experiments]
      (random-run "~/repos/notional/supplyvariation-testdata.xlsx"
                  :reps 1
                  :phases phases
                  :lower 0 :upper 1.5
                  :compo-lengths default-compo-lengths)))

  #_
  (def random-run-exhaustive
    (binding [*noisy* 1.0
              *project->experiments* project->full-factorial-experiments]
      (random-run "~/repos/notional/supplyvariation-testdata.xlsx"
                  :reps 1
                  :phases phases
                  :lower 0 :upper 1.5
                  :compo-lengths default-compo-lengths)))

  ;;some defaults...
  #_
  (def phases [["comp-1"  1   730]
               ["phase-1" 731 763]
               ["phase-2" 764 883]
               ["phase-3" 884 931]
               ["phase-4" 932 1699]
               ["comp-2"  1700 2430]]))

