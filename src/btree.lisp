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
  (if (and (null key) (null value))
      (%update-btree btree)
      (let ((root (btree-root btree)))
	(%update-btree 
	 btree 
	 :root (cond 
		 ((not root) 
		  (make-root-node btree key value))
		 ((node-almost-full-p btree root)
		  (update-node 
		   root :index (split-binding-node 
				btree root key value nil)))
		 (t	 
		  (update-node 
		   root :index (update-index-for-insert 
				btree (btree-node-index root) 
				key value (btree-node-leaf-p root)))))))))

(defclass btree-node ()
  ((index :initarg :index
	  :initform (vector)
	  :accessor btree-node-index
	  :documentation "A vector of key/value pairs.  The keys are
 sorted by KEY<. No two keys can be the same.  For leaf nodes of btrees
 with non-unique-keys, the value part is actually a list of values.
 For intermediate nodes, the value is a child node.  All keys in the
 child node will be KEY< the child node's key in the parent node.")
   (leaf-p :initarg :leaf-p :initform nil :accessor btree-node-leaf-p)))

(defmethod print-object ((object btree-node) stream)
  (print-unreadable-object (object stream :type t)
    (format stream "~A" (btree-node-index object))))

(defmethod update-node (node &key (index (btree-node-index node))
			(leaf-p (btree-node-leaf-p node)))		    
  (make-instance (class-of node) :index index :leaf-p leaf-p))

(defmethod make-root-node (btree key val)
  (let* ((left (update-node (make-instance (btree-node-class btree) 
					   :leaf-p t 
					   :index (vector (funcall 
							   (if (btree-unique-keys-p btree)
							       'cons
							       'list)
							   key val))))))
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
	       (funcall (btree-key< btree) key bkey)
	       (funcall (btree-key= btree) key bkey))

     :do (return i)
     :finally (return i)))

(defun btree-node-binding (node i)
   (aref (btree-node-index node) i))

(defmethod btree-node-binding-key (node binding)
  (car binding))

(defmethod btree-node-binding-value (node binding)
  (cdr binding))

 
(defmethod largest-key-in-node (node)
  (let ((index (btree-node-index node))
	(leaf-p (btree-node-leaf-p node)))
    (if leaf-p
	(car (aref index (1- (length index))))
	(let ((node (cdr (aref index (1- (length index))))))
	  (largest-key-in-node node)))))


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
	      (vector (cons key (if (btree-unique-keys-p btree)
				    value
				    (cons value node)

				    )))
	      ;;new key
	      (sort (vector (funcall (if (btree-unique-keys-p btree)
					 'cons 
					 'list)
				     key value) 
			    (cons binding-key node))
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
  (update-btree btree :key key :value value :action :insert))

