(in-package :planks.btree)

(defclass file-btree (btree)
  ((pathname :accessor btree-pathname :initarg :pathname))
  (:default-initargs :node-class 'file-btree-node))

(defparameter *btree-file-root* (ensure-directories-exist #P"/tmp/pp-btree/"))

(defclass file-btree-node (btree-node)
  ((address :accessor btree-node-address :initarg :address)))

(defvar *btree-stream*)

(defun load-btree-node (btree address)
  (typecase address
    (file-btree-node address)
    (integer 
     (alexandria:with-input-from-file (s (btree-pathname btree) 
					 :element-type '(unsigned-byte 8))
       (file-position s address) 
       (let ((instance (allocate-instance (find-class (btree-node-class btree)))))
	 (prog1 instance
	   (setf (btree-node-address instance) address
		 (btree-node-leaf-p instance) (rs::deserialize s)
		 (btree-node-index instance)
		 (let ((size (rs::deserialize s)))
		   (loop 
		      :with vector = (make-array size) for n from 0 to (1- size)
		      :do (setf (aref vector n) (cons (rs::deserialize s) (rs::deserialize s)))
		      :finally (return vector))))))))))
  

(defmethod persist (node &key (stream *btree-stream*))
  (declare (special %btree%))
  (force-output stream)
  (assert (not (slot-boundp node 'address)) ()
	  "Node is already persisted, fail!")
  (let* ((eof (file-length stream)))
    (setf (btree-node-address node) eof)
    (file-position stream eof)
    (rs::serialize (btree-node-leaf-p node) stream)
    (rs::serialize (length (btree-node-index node)) stream)
    (loop :for (key . value) :across (btree-node-index node) 
       :do 
       (rs::serialize key stream)
       (rs::serialize value stream))       		   
    (force-output stream)))

(defmethod rs::serialize ((object file-btree-node) stream)
  (rs::serialize (btree-node-address object) stream))

(defun btree-node-file-position (node)
  (typecase node 
    (integer node)
    (file-btree-node (btree-node-address node))))

(defmethod btree-root ((btree file-btree))
  (let ((root (call-next-method)))
    (when root 
      (load-btree-node btree root))))
	

(defmethod update-node-for-insert (btree (pointer integer) binding-key key value (leaf-p null))
  (call-next-method btree (load-btree-node btree pointer) binding-key key value leaf-p))

(defmethod btree-depths :around ((btree file-btree))
  (let ((%btree% btree))
    (declare (special %btree%))
  (if (slot-value btree 'root)
      (values (btree-node-min-depth (btree-root btree))
              (btree-node-max-depth (btree-root btree)))
    (values 0 0))))

(defmethod btree-node-min-depth ((node integer))
  (declare (special %btree%))
  (btree-node-min-depth (load-btree-node %btree%  node)))

(defmethod btree-node-max-depth ((node integer))
  (declare (special %btree%))
  (btree-node-max-depth (load-btree-node %btree% node)))

(defmethod largest-key-in-node ((pointer integer))
  (declare (special %btree%))
  (largest-key-in-node (load-btree-node %btree% pointer)))

(defmethod node-search (btree (node integer) key errorp default-value)
  (node-search btree (load-btree-node btree node) key errorp default-value))

(defmethod map-btree-node (btree function (pointer integer))
  (call-next-method function (load-btree-node btree pointer)))

(defmethod map-btree-keys-for-node :around (btree (pointer integer) function min max include-min include-max order)
  (call-next-method btree (load-btree-node btree pointer) function min max include-min include-max order))
          

(defmethod update-node :around ((node file-btree-node) &key &allow-other-keys)
  (let ((new-node (call-next-method)))
    (prog1 new-node 
      (persist new-node))))

(defmethod update-btree :around ((btree file-btree) &key &allow-other-keys)
  (let ((%btree% btree))
    (declare (special %btree%))
    (let ((new-btree (call-next-method)))
      (prog1 new-btree 
	(setf (btree-pathname new-btree)
	      (btree-pathname btree)
	      (btree-root btree)
	      (btree-root btree))))))

(defclass single-file-btree (file-btree)
  ((footer :accessor btree-file-footer)
   (footer-class :accessor btree-file-footer-class :initarg :footer-class :initform 'btree-footer)
   (lock :accessor btree-lock)))

(#+sbcl sb-ext:defglobal #-sbcl defvar =big-btree-lock= (bordeaux-threads:make-lock "Big lock for btrees"))

(defvar *btrees* (make-hash-table :test #'equal :synchronized t))

(defclass btree-footer ()
  ((root-node-address :initform nil :initarg :root-node-position
		      :accessor root-node-file-position)
   (address :initarg :address :accessor btree-file-footer-address)
   
   (action :initform :create :initarg :action :accessor btree-file-footer-action)
   (version :initform 0 :accessor btree-file-footer-version :initarg :version)
   (previous-address :accessor btree-file-footer-previous-address :initarg :previous-address)
   (key :initform nil 
	:accessor btree-file-footer-key)))

(defmethod make-btree-footer ((btree single-file-btree) old-footer &key (action :create))
  (make-instance 
   (btree-file-footer-class btree)
   :action action
   :root-node-position (when (btree-root btree)
			 
			 (btree-node-file-position 
			  (btree-root btree)))
   :version (if old-footer (1+ (btree-file-footer-version old-footer)) 0)
   :previous-address (when old-footer (btree-file-footer-previous-address old-footer))))

(defun object-checksum (header)
  (let* ((rs::*default-buffer-size* 512)
	 (buffer (make-instance 'rs::serialization-buffer)))
    (rs::serialize header buffer)
    (ironclad:digest-sequence :md5 (rs::contents buffer))))

(defvar +footer-marker+ rs::+ILLEGAL-MARKER+)

(defun write-file-footer (stream footer)
  (let* ((checksum (object-checksum footer)))
    (force-output stream)
    (file-position stream (file-length stream))
    (dotimes (n 2)
      (write-byte +footer-marker+ stream)
      (rs::serialize footer stream)
      (rs::serialize checksum stream)))
  (force-output stream))

(defun read-file-footer (stream &key (count 256) (start (- (file-length stream) count)) )
  (file-position stream (setf start (if (>= start 0)
					start
					0)))

  (let ((footer (loop 
		   :for n from 1 to count
		   :for byte = (read-byte stream nil)
		   :while byte
		   :when (eql byte +footer-marker+)
		   :do 
		   (let ((pos (file-position stream))	  
			 (footer (ignore-errors (rs::deserialize stream))))
		      (if  footer
			   (let ((checksum (ignore-errors (rs::deserialize stream))))
			     (if  (and checksum (equalp checksum (object-checksum footer)))
				  (return footer)
				  (file-position stream pos)))
			   (file-position stream pos))))))
    (or footer 
	(if (not (zerop start))
	    (read-file-footer stream :start (- start count) :count count)
	    (error "FATAL: Can't read btree footer")))))

(defun make-btree-lock (btree)
   (bordeaux-threads:make-lock (format nil "BTREE lock for ~A" (btree-pathname btree))))

(defun make-btree (pathname &rest args &key (if-exists :error) 
		                            (class 'single-file-btree) &allow-other-keys)
  (bordeaux-threads:with-lock-held (=big-btree-lock=)
    (with-open-file (stream pathname
			    :if-does-not-exist :create
			    :if-exists if-exists
			    :direction :output
			    :element-type '(unsigned-byte 8))
      (let* ((btree (apply 'make-instance class
			  (alexandria:remove-from-plist args :if-exists :class)))
	    (footer (make-btree-footer btree nil)))
	
	(setf (btree-pathname btree) pathname)
	(rs::serialize btree stream)
	(write-file-footer stream footer)
	(force-output stream)
	(setf (btree-file-footer btree) footer)
	(setf (btree-lock btree) (make-btree-lock btree))
	(setf (gethash pathname *btrees*) btree)))))

(defun read-btree-from-file-stream (stream)
  (file-position stream 0)
  (let* ((btree (rs::deserialize stream))
	 (footer (read-file-footer stream))
	 (root-position (root-node-file-position footer)))
    (setf (btree-file-footer btree) footer)
    (when root-position 
      (file-position stream root-position)
      (setf (btree-root btree)
	    (rs::deserialize stream)))
    btree))

(defun find-btree (path)
    (or (gethash path *btrees*) 
	(bordeaux-threads:with-lock-held (=big-btree-lock=)
	  (or (gethash path *btrees*) 
	      (setf (gethash path *btrees*) 
		      (with-open-file (s path 
					 :if-does-not-exist :error		     
					 :direction :input
					 :element-type '(unsigned-byte 8))
			(let ((btree (read-btree-from-file-stream s)))
			  (setf (btree-lock btree) (make-btree-lock btree))
			  (setf (btree-root btree) (load-btree-node btree (root-node-file-position (btree-file-footer btree))))
				
			  btree)))))))

(defun close-btree (path)
  (bordeaux-threads:with-lock-held (=big-btree-lock=)
    (remhash path *btrees*)))
  
(defmethod update-btree :around ((btree single-file-btree) &rest args 
				 &key  
				 &allow-other-keys)
  (let* ((current-btree (find-btree (btree-pathname btree)))
	 (lock (btree-lock btree)))
    (if (equal (root-node-file-position (btree-file-footer btree))
	       (root-node-file-position (btree-file-footer current-btree)))
	(bordeaux-threads:with-recursive-lock-held (lock)
	  (with-open-file (*btree-stream* (btree-pathname btree)
					  :if-does-not-exist :error
					  :if-exists :overwrite
					  :direction :output
					  :element-type '(unsigned-byte 8))
	    (let ((btree (call-next-method)))	       
	      (prog1 btree
		(setf (btree-file-footer-class btree) (btree-file-footer-class current-btree))
		(let ((footer  (apply #'make-btree-footer 
				      btree (btree-file-footer current-btree) 
				      args)))
		  (setf (btree-file-footer-address footer) (file-position *btree-stream*))
		  (write-file-footer *btree-stream* footer)
		  (setf (btree-file-footer btree) footer)
		  (setf (btree-lock btree) lock)
		  (setf (gethash (btree-pathname btree) *btrees*) btree))))))
	(apply #'update-btree current-btree args))))
      






   