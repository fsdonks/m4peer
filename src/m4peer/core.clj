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

