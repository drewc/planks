(in-package :planks.btree)

(defclass heap-btree (multi-btree)
  ((heap-size :initarg :heap-size 
	      :initform (* 1024 1024)
	      :accessor btree-heap-size)
   (heap-start :initform 0)
   (free-space-start :initform 0))
  (:default-initargs :footer-class 'heap-btree-file-footer))

(defclass heap-btree-file-footer (multi-btree-file-footer)
  ((heap-start :initform 0)
   (free-space-start :initform 0)))

(defmethod btree-heap-start ((btree heap-btree))
  (slot-value (btree-file-footer btree) 'heap-start))

(defmethod btree-heap-free-space-start ((btree heap-btree))
  (slot-value (btree-file-footer btree) 'free-space-start))

(defmethod make-btree-footer :around ((btree heap-btree) old-footer 
				      &key 
				      &allow-other-keys)
  (let ((footer (call-next-method)))
    (prog1 footer 
      (with-slots (heap-start free-space-start) btree
;	(break "~A bts~A btf~A" t heap-start free-space-start)
	(with-slots ((hs heap-start) (fss free-space-start)) footer
	    (setf hs heap-start
		  fss free-space-start))))))
	
(defmethod allocate-heap ((btree heap-btree) start stream)
  (file-position *btree-stream* start) 
  (loop repeat (slot-value btree 'heap-size) do (write-byte rs::+FREE-BLOCK+ stream))
  start)

(defmethod allocate-object ((btree heap-btree) object object-start heap-start stream)
  (let* ((buffer (make-instance 'rs::serialization-buffer))
	 (object-length (progn (rs::serialize object buffer) 
			       (length (slot-value buffer 'rs::contents))))
	 (free-space (- (slot-value btree 'heap-size) 
			(- object-start heap-start))))
    (assert (> (btree-heap-size btree) object-length) ()
	    "Objects larger then heap size not supported")
    (when (> object-length free-space)
      (setf heap-start 
	    (setf object-start (allocate-heap btree (file-length stream) stream))))
    (rs::save-buffer buffer stream :file-position object-start)
    (values object-start (file-position stream) heap-start)))

(defmethod find-heap-object (heap address)
  (alexandria:with-input-from-file (s (btree-pathname heap) :element-type '(unsigned-byte 8))
    (file-position s address) 
    (rs::deserialize s)))

      
(defmethod update-btree ((btree heap-btree) &rest args &key value action)
  (assert (sb-thread:holding-mutex-p (btree-lock btree)))
  (let ((start (btree-heap-start btree))
	(free (btree-heap-free-space-start btree)))

    (unless (btree-root btree)
      (setf start (file-length *btree-stream*))
      (setf free start)
      (allocate-heap btree start *btree-stream*))

    (case action 
      (:insert 
       (multiple-value-setq (value free start) (allocate-object btree value free start *btree-stream*))))

    (let ((new-tree (apply #'call-next-method btree :value value args)))
      (prog1 new-tree
	(setf (slot-value new-tree 'heap-size) (btree-heap-size btree))
	(setf (slot-value new-tree 'heap-start) start)
	(setf (slot-value new-tree 'free-space-start) free)))))

(defmethod btree-search ((btree heap-btree) key &key address-only &allow-other-keys)
  (let ((address (call-next-method)))
    (if address-only address (if (btree-unique-keys-p btree)
				 (find-heap-object btree address)
				 (mapcar (lambda (a) (find-heap-object btree a)) address)))))


(defmethod map-btree :around ((bt heap-btree) fn &rest args &key address-only &allow-other-keys)
  (let ((fun (if address-only 
		 fn 
		 (if (btree-unique-keys-p bt)						  
		     (lambda (k address) 
		       (funcall fn k (find-heap-object bt address)))
		     (lambda (k address) 
		       (funcall fn k (mapcar (lambda (a) (find-heap-object bt a)) address)))))))	       
    (apply #'call-next-method bt fun args)))
  
      
      
      

  
   

