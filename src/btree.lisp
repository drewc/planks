(in-package :planks.btree)


(defclass btree ()
  ((key<   :initarg :key<   :reader btree-key<   :initform '<)
   (key=   :initarg :key=   :reader btree-key=   :initform 'eql)
   (value= :initarg :value= :reader btree-value= :initform 'eql)
   (key-key :initarg :key-key :reader btree-key-key :initform 'identity
            :documentation "A unary function that is applied to a
btree key before comparing it to another key with a key comparison
predicate like BTREE-KEY<.")
   (value-key :initarg :value-key :reader btree-value-key :initform 'identity
              :documentation "A unary function that is applied to a
btree value before comparing it to another value with the BTREE-VALUE=
predicate.")
   ;;
   (node-class :initarg :node-class
	       :reader btree-node-class
	       :initform 'btree-node)
   (max-node-size :initarg :max-node-size
		  :reader btree-max-node-size
		  :initform 100
		  :documentation "An integer specifying the preferred maximum number
of keys per btree node.")
   (unique-keys-p :initarg :unique-keys-p
		  :reader btree-unique-keys-p
		  :initform t
		  :documentation "If false, one key can correspond to more than one value.")
   (key-type :initarg :key-type
	     :reader btree-key-type
	     :initform t
	     :documentation "The type of all keys.")
   (value-type :initarg :value-type
	       :reader btree-value-type		 :initform t
	       :documentation "The type of all values.")
   (root :accessor btree-root :initform nil)))

;;
;; Comparison functions that can be deduced from KEY< (because the
;; btree keys have a total order).
;;

(defmethod btree-key< ((btree btree))
  (let ((key< (slot-value btree 'key<))
        (key-key (btree-key-key btree)))
    (lambda (key1 key2)
      (and (not (eql key1 'key-irrelevant))
           (not (eql key2 'key-irrelevant))
           (funcall key<
                    (funcall key-key key1)
                    (funcall key-key key2))))))

(defmethod btree-key= ((btree btree))
  (let ((key< (slot-value btree 'key<))
        (key-key (btree-key-key btree)))
    (lambda (key1 key2)
      (and (not (eql key1 'key-irrelevant))
           (not (eql key2 'key-irrelevant))
           (let ((key1 (funcall key-key key1))
                 (key2 (funcall key-key key2)))
             (and (not (funcall key< key1 key2))
                  (not (funcall key< key2 key1))))))))

(defmethod btree-key>= ((btree btree))
  (lambda (key1 key2)
    (not (funcall (btree-key< btree) key1 key2))))

(defmethod btree-key<= ((btree btree))
  (let ((key< (slot-value btree 'key<))
        (key-key (btree-key-key btree)))
    (lambda (key1 key2)
      (let ((key1 (funcall key-key key1))
            (key2 (funcall key-key key2)))
        (or (funcall key< key1 key2)
            (not (funcall key< key2 key1)))))))

(defmethod btree-key> ((btree btree))
  (let ((key< (slot-value btree 'key<))
        (key-key (btree-key-key btree)))
    (lambda (key1 key2)
      (let ((key1 (funcall key-key key1))
            (key2 (funcall key-key key2)))
        (and (not (funcall key< key1 key2))
             (funcall key< key2 key1))))))


(defmethod btree-value= ((btree btree))
  (let ((value= (slot-value btree 'value=))
        (value-key (btree-value-key btree)))
    (lambda (value1 value2)
      (let ((value1 (funcall value-key value1))
            (value2 (funcall value-key value2)))
        (funcall value= value1 value2)))))


(defun %update-btree (btree &key (root (slot-value btree 'root)))
  (LET ((NEW (allocate-INSTANCE (CLASS-OF btree))))
    (PROG1 NEW
      (SETF (SLOT-VALUE NEW 'KEY<) (SLOT-VALUE btree 'KEY<)
            (SLOT-VALUE NEW 'KEY=) (SLOT-VALUE btree 'KEY=)
	    (SLOT-VALUE NEW 'KEY-KEY) (SLOT-VALUE btree 'KEY-KEY)
	    (SLOT-VALUE NEW 'VALUE-KEY) (SLOT-VALUE btree 'VALUE-KEY)	    
            (SLOT-VALUE NEW 'VALUE=) (SLOT-VALUE btree 'VALUE=)
            (SLOT-VALUE NEW 'NODE-CLASS) (SLOT-VALUE btree 'NODE-CLASS)
            (SLOT-VALUE NEW 'MAX-NODE-SIZE) (SLOT-VALUE btree 'MAX-NODE-SIZE)
            (SLOT-VALUE NEW 'UNIQUE-KEYS-P) (SLOT-VALUE btree 'UNIQUE-KEYS-P)
            (SLOT-VALUE NEW 'KEY-TYPE) (SLOT-VALUE btree 'KEY-TYPE)
            (SLOT-VALUE NEW 'VALUE-TYPE) (SLOT-VALUE btree 'VALUE-TYPE)
            (SLOT-VALUE NEW 'ROOT) root))))

(defmethod update-btree (btree &key key value)
  (let ((root (btree-root btree)))
    (%update-btree 
     btree 
     :root (cond 
	     ((not root) 
	      (make-root-node btree key value))
	     ((node-almost-full-p btree root)
	      (update-node 
	       root :index (split-binding-node 
			    btree  root key value nil)))
	     (t	 
	      (update-node 
	       root :index (update-index-for-insert 
			    btree (btree-node-index root) 
			    key value (btree-node-leaf-p root))))))))

(defclass btree-node ()
  ((index :initarg :index
          :initform (vector)
          :accessor btree-node-index
          :documentation "A vector of key/value pairs.  The keys are
sorted by KEY<. No two keys can be the same.  For leaf nodes of btrees
with non-unique-keys, the value part is actually a list of values.
For intermediate nodes, the value is a child node.  All keys in the
child node will be KEY< the child node's key in the parent node.")
   (leaf-p :initarg :leaf-p :initform nil :reader btree-node-leaf-p)))

(defmethod print-object ((object btree-node) stream)
  (print-unreadable-object (object stream :type t)
    (format stream "~A" (btree-node-index object))))

(defmethod update-node (node &key (index (btree-node-index node))
			          (leaf-p (btree-node-leaf-p node)))		    
  (make-instance (class-of node) :index index :leaf-p leaf-p))

(defmethod make-root-node (btree key val)
  (let* ((left (update-node (make-instance (btree-node-class btree) :leaf-p t :index (vector (cons key val))))))
    (update-node (make-instance (btree-node-class btree) 
		   :index (vector (cons key left))
		   :leaf-p nil))))

(defun node-almost-full-p (btree node)
  (>= (length (btree-node-index node)) (1- (btree-max-node-size btree))))

(defun find-key-position-in-index (btree index key)
  "Returns the position of the subnode that contains more information for the given key."

  ;; Find the first binding with a key >= the given key and return
  ;; the corresponding subnode.
  ;; DO: We should probably use binary search for this.
  (loop 
     :for (bkey . value) :across index
     :for i from 0
     :when (or (= (1+ i) (length index))
	       (funcall (btree-key= btree) key bkey)
	       (funcall (btree-key< btree) key bkey))
	       
     :do (return i)
     :finally (return i)))

(defun btree-node-binding (node i)
  (aref (btree-node-index node) i))

(defmethod btree-node-binding-key (node binding)
  (car binding))

(defmethod btree-node-binding-value (node binding)
  (cdr binding))

(defun btree-node-binding-count (node)
  (length (btree-node-index node)))

(defmethod largest-key-in-node (node)
  (let ((index (btree-node-index node))
	(leaf-p (btree-node-leaf-p node)))
    (if leaf-p
	(car (aref index (1- (length index))))
	(let ((node (cdr (aref index (1- (length index))))))
	  (largest-key-in-node node)))))

(defmethod btree-node-max-depth ((node btree-node))
  (if (btree-node-leaf-p node)
      0
    (loop for i below (btree-node-binding-count node)
          for binding = (btree-node-binding node i)
          maximize (1+ (btree-node-max-depth (btree-node-binding-value 
					node binding))))))

(defmethod btree-node-min-depth ((node btree-node))
  (if (btree-node-leaf-p node)
      0
    (loop for i below (btree-node-binding-count node)
          for binding = (btree-node-binding node i)
          minimize (1+ (btree-node-min-depth (btree-node-binding-value 
					      node binding))))))

(defmethod btree-depths ((btree btree))
  (if (slot-value btree 'root)
      (values (btree-node-min-depth (btree-root btree))
              (btree-node-max-depth (btree-root btree)))
    (values 0 0)))

(defmethod btree-balanced-p ((btree btree))
  (multiple-value-bind (min max)
      (btree-depths btree)
    (<= (- max min) 1)))


(defun update-bnode (btree node key value)
  (update-node 
   node :index (update-index-for-insert 
		btree (btree-node-index node) 
		key value (btree-node-leaf-p node))))

(defun update-index-for-insert (btree index key value leaf-p)
  (cond ((eql 0 (length index))
	 (vector (cons key value)))
	(t 
	 (let* ((pos (find-key-position-in-index btree index key))
		(binding (aref index pos))
		(left-index (unless (eql 0 pos) 
			      (make-array pos :displaced-to index)))
		(right-index (unless (<= (length index) (1+ pos)) 
			       (make-array (1- (- (length index) pos))
					       :displaced-to index
					       :displaced-index-offset (1+ pos)))))
	   (concatenate 'vector 
			left-index 
			(update-binding-for-insert btree binding key value leaf-p)
			right-index)))))

(defmethod update-node-for-insert (btree node binding-key key value leaf-p)
  (if (and (not leaf-p) (node-almost-full-p btree node))
	(split-binding-node btree node key value (btree-node-leaf-p node))
	(if leaf-p 
	    (if (funcall (btree-key= btree) binding-key key)
		;; replacing existing binding
		(vector (cons key value))
		;;new key
		(sort (vector (cons key value) (cons binding-key node))
		      (btree-key< btree) :key #'car))
	    (vector (cons binding-key (update-bnode btree node key value))))))

(defun update-binding-for-insert (btree binding key value leaf-p)
  (destructuring-bind (binding-key . node) binding
    (update-node-for-insert btree node binding-key key value leaf-p)))

(defun split-binding-node (btree node key value leaf-p)
  (let* ((node-index (btree-node-index node))
	 (node-pos (find-key-position-in-index btree node-index key))
	 (split-pos (floor (length node-index) 2))
	 (split-left-index (subseq node-index 0 (1+ split-pos)))
	 (split-right-index (subseq node-index (1+ split-pos)))
	 (sub-index (if (<= (1+ node-pos) (length split-left-index))
			split-left-index
			split-right-index))
	 (left-node (update-node 
		     node :index (if (eql split-left-index sub-index)
				     (update-index-for-insert btree split-left-index key value leaf-p)
				     split-left-index)))
	 (right-node (update-node 
		      node :index (if (eql split-right-index sub-index)
				      (update-index-for-insert btree split-right-index key value leaf-p)
				      split-right-index))))

    
	(vector (cons (largest-key-in-node left-node) left-node)
		(cons (largest-key-in-node right-node) right-node))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Search
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defgeneric btree-search (btree key &key errorp default-value)
  (:documentation "Returns the value (or list of values, for btrees
that don't have unique keys) corresponding to KEY.  If the btree has
non-unique keys and no value is found, the empty list is returned.  If
the btree has unique keys and no value is found, the result depends on
ERRORP option: if ERRORP is true, a btree-search-error is signalled;
otherwise, DEFAULT-VALUE is returned."))


(defmethod btree-search (btree key &key (errorp t) (default-value nil))
  (if (slot-value btree 'root)
      (node-search btree (slot-value btree 'root) key errorp default-value)
    (not-found btree key errorp default-value)))


(defun not-found (btree key errorp default-value)
  (if (btree-unique-keys-p btree)
      (if errorp
          ;; DO: Provide restarts here (USE-VALUE, STORE-VALUE, ...).
          (error 'btree-search-error :btree btree :key key)
	  default-value)
    '()))

;;
;; Node-search
;;
 
(defgeneric node-search (btree node key errorp default-value)
  (:method ((btree btree) (node btree-node) key errorp default-value)
   (if (btree-node-leaf-p node)
       (let ((binding (find key (btree-node-index node)
                            :key #'car
                            :test (btree-key= btree))))
         (if binding
             (cdr binding)
           (not-found btree key errorp default-value)))
     (let ((subnode (find-subnode btree node key)))
       (node-search btree subnode key errorp default-value)))))


(defun find-subnode (btree node key)
  "Returns the subnode that contains more information for the given key."
  (let* ((pos (find-key-position-in-index btree (btree-node-index node) key))
	(binding (aref (btree-node-index node) pos)))
    (cdr binding)))

;;;; iteration

(defmethod map-btree-node (function node)
  (funcall function node)
  (unless (btree-node-leaf-p node)
    (loop for (key . node) across (btree-node-index node) do (map-btree-node function node))))

(defun map-btree-nodes (function btree)
    (map-btree-node function (btree-root btree)))

(defun map-node-bindings (function node)
  (loop for (key . value) across (btree-node-index node) do (funcall function key value)))


(defmethod btree-insert ((btree btree) key value &key (if-exists :overwrite))
  ;; Check that key and value are of the right type.
  (unless (typep key (btree-key-type btree))
    (error 'btree-type-error
           :btree btree
           :datum key
           :expected-type (btree-key-type btree)))
  (unless (typep value (btree-value-type btree))
    (error 'btree-type-error
           :btree btree
           :datum value
           :expected-type (btree-value-type btree)))
  ;; Do the real work.
  (update-btree btree :key key :value value))

(defmethod btree-delete ((btree btree) key value
                         &key (if-does-not-exist :ignore))
  (flet ((forget-it ()
           ;; Signal an error or return quietly.
           (ecase if-does-not-exist
             (:ignore (return-from btree-delete))
             (:error (error 'btree-deletion-error
                            :btree btree
                            :key key
                            :value value)))))
    (multiple-value-bind (binding node position)
        (and (slot-boundp btree 'root)
             (node-search-binding btree (btree-root btree) key))
      (cond ((not binding)
             ;; The binding doesn't exist: forget it.
             (forget-it))
            ((btree-unique-keys-p btree)
             (if (funcall (btree-value= btree) value (binding-value binding))
                 ;; The binding exists and it has the right value: let
                 ;; BTREE-DELETE-KEY do the real work.
                 (btree-delete-key btree key)
               ;; The binding exists but it has the wrong value: forget it.
               (forget-it)))
            (t
             ;; For non-unique keys, we ignore the :IF-EXISTS option and
             ;; just delete the value from the list of values (unless it's
             ;; not there).
             (flet ((check (x) (funcall (btree-value= btree) x value)))
               (let ((set (binding-value binding)))
                 (etypecase set
                   (persistent-object-set
                    (set-btree-delete set value :if-does-not-exist :ignore)
                    (when (set-btree-empty-p set)
                      ;; This was the last value in the set: remove the key.
                      (btree-delete-key btree key)))
                   ((or null persistent-cons)
                    (if (p-find value set :test (btree-value= btree))
                        (if (null (p-cdr set))
                            ;; This is the last value in the list: remove the
                            ;; key.
                            (btree-delete-key btree key)
                          ;; There's more than one value in the list: delete the
                          ;; value that must be deleted and keep the other values.
                          (setf (node-binding-value node position)
                                (p-delete-if #'check (binding-value binding)
                                             :count 1)))
                      ;; The value is not in the list: forget it.
                      (forget-it)))))))))))