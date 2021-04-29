(ns jepsen.control.clj-ssh
  "A CLJ-SSH powered implementation of the Remote protocol."
  (:require [clojure.tools.logging :refer [info warn]]
            [clj-ssh.ssh :as ssh]
            [jepsen.control.remote :as remote]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util.concurrent Semaphore)))

(def clj-ssh-agent
  "Acquiring an SSH agent is expensive and involves a global lock; we save the
  agent and re-use it to speed things up."
  (delay (ssh/ssh-agent {})))

(defn clj-ssh-session
  "Opens a raw session to the given connection spec"
  [conn-spec]
  (let [agent @clj-ssh-agent
        _     (when-let [key-path (:private-key-path conn-spec)]
                (ssh/add-identity agent {:private-key-path key-path}))]
    (doto (ssh/session agent
                       (:host conn-spec)
                       (select-keys conn-spec
                                    [:username
                                     :password
                                     :port
                                     :strict-host-key-checking]))
      (ssh/connect))))

; TODO: pull out dummy logic into its own remote
(defrecord Remote [concurrency-limit
                   session
                   semaphore]
  remote/Remote
  (connect [this conn-spec]
    (assert (map? conn-spec)
            (str "Expected a map for conn-spec, not a hostname as a string. Received: "
                 (pr-str conn-spec)))
    (assoc this :session (if (:dummy conn-spec)
                           {:dummy true}
                           (try+
                            (clj-ssh-session conn-spec)
                            (catch com.jcraft.jsch.JSchException _
                              (throw+ (merge conn-spec
                                             {:type :jepsen.control/session-error
                                              :message "Error opening SSH session. Verify username, password, and node hostnames are correct."})))))
           :semaphore (Semaphore. concurrency-limit true)))

  (disconnect! [_]
    (when-not (:dummy session) (ssh/disconnect session)))

  (execute! [_ action]
    (when-not (:dummy session)
      (.acquire semaphore)
      (try
        (ssh/ssh session action)
        (finally
          (.release semaphore)))))

  (upload! [_ local-paths remote-path rest]
    (when-not (:dummy session)
      (apply ssh/scp-to session local-paths remote-path rest)))

  (download! [_ remote-paths local-path rest]
    (when-not (:dummy session)
      (apply ssh/scp-from session remote-paths local-path rest))))

(def concurrency-limit
  "OpenSSH has a standard limit of 10 concurrent channels per connection.
  However, commands run in quick succession with 10 concurrent *also* seem to
  blow out the channel limit--perhaps there's an asynchronous channel teardown
  process. We set the limit a bit lower here. This is experimentally determined
  for clj-ssh by running jepsen.control-test's integration test... <sigh>"
  8)

(defn remote
  "A remote that does things via clj-ssh."
  []
  (Remote. concurrency-limit nil nil))