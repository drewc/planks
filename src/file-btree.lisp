(in-package :planks.btree)

(defclass file-btree (btree)
  ((pathname :accessor btree-pathname :initarg :pathname))
  (:default-initargs :node-class 'file-btree-node))

(defparameter *btree-file-root* (ensure-directories-exist #P"/tmp/pp-btree/"))

(defclass file-btree-node (btree-node)
  ((self-pointer :accessor btree-node-self-pointer :initarg :pointer)))

(defclass pointer ()
  ((address :accessor pointer-address :initarg :address)
   (btree-file-path :accessor pointer-btree-file-path :initarg :pathname)))

(defvar *btree-stream*)

(defconstant +btree-node-marker+ #xC1)

(defgeneric maybe-follow-pointer (possible-pointer))

(defmethod maybe-follow-pointer (object) object)
(defmethod maybe-follow-pointer ((pointer pointer))
  (with-open-file (stream (pointer-btree-file-path pointer)
			  :if-does-not-exist :error	
			  :direction :input
			  :element-type '(unsigned-byte 8))
    (file-position stream (pointer-address pointer))
    (rs::deserialize stream)))

(defun btree-node-file-position (node)
  (pointer-address 
   (btree-node-self-pointer 
    (maybe-follow-pointer node))))

(defmethod btree-root ((btree file-btree))
  (maybe-follow-pointer (call-next-method)))

(defmethod update-node-for-insert (btree (pointer pointer) binding-key key value leaf-p)
  (call-next-method btree (maybe-follow-pointer pointer) binding-key key value leaf-p))

(defmethod largest-key-in-node ((pointer pointer))
  (largest-key-in-node (maybe-follow-pointer pointer)))

(defmethod node-search (btree (node pointer) key errorp default-value)
  (node-search btree (maybe-follow-pointer node) key errorp default-value))

(defmethod map-btree-node (function (pointer pointer))
  (call-next-method function (maybe-follow-pointer pointer)))

(defmethod map-btree-keys-for-node :around (btree (pointer pointer) function min max include-min include-max order)
  (call-next-method btree (maybe-follow-pointer pointer) function min max include-min include-max order))
          
(defmethod rs::serialize ((object pointer) stream)
  (rs::serialize-marker +btree-node-marker+ stream)
  (rs::serialize-standard-object object stream))

(defmethod rs::serialize ((object file-btree-node) stream)
  (rs::serialize (btree-node-self-pointer object) stream))

(defmethod rs::deserialize-contents ((marker (eql +btree-node-marker+)) stream)
  (rs::deserialize stream))

(defmethod persist (node &key (stream *btree-stream*))
  (declare (special %btree%))
  (force-output stream)
  (assert (not (slot-boundp node 'self-pointer)) ()
	  "Node is already persisted, fail!")
  (let* ((eof (file-length stream))
	 (pointer (make-instance 'pointer 
				 :pathname (btree-pathname %btree%)
				 :address eof)))
    (setf (btree-node-self-pointer node) pointer)
    (file-position stream eof)
    (rs::serialize-standard-object node stream)
    (force-output stream)))

(defmethod update-node :around ((node file-btree-node) &key &allow-other-keys)
  (let ((new-node (call-next-method)))
    (prog1 new-node 
      (persist new-node))))

(defmethod update-btree :around ((btree file-btree) &key &allow-other-keys)
  (let ((%btree% btree))
    (declare (special %btree%))
    (let ((new-btree (call-next-method)))
      (prog1 new-btree 
	(setf (btree-root new-btree) 
	      (btree-node-self-pointer (btree-root new-btree)))
	(setf (btree-pathname new-btree)
	      (btree-pathname btree))))))

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

(defvar *footer-marker* (babel:string-to-octets "Btree Footer : v0.1"))
(defvar *footer-length* 1024)

(defun write-file-footer (stream footer)
  (let* ((rs::*default-buffer-size* (- *footer-length* (length *footer-marker*)))
	 (buffer (make-instance 'rs::serialization-buffer))
	 (checksum (object-checksum footer)))
    (rs::serialize  footer buffer)
    (rs::serialize checksum buffer)
    (setf (fill-pointer (rs::contents buffer)) rs::*default-buffer-size*)
    (force-output stream)
    (file-position stream (file-length stream))
    (write-sequence *footer-marker* stream)
    (rs::save-buffer buffer stream)
    (force-output stream)
    (write-sequence *footer-marker* stream)
    (rs::save-buffer buffer stream)
    (force-output stream)))

(defun read-file-footer (stream &optional (file-position (- (file-length stream) *footer-length*)))
  (when file-position (file-position stream file-position))
  (let ((marker (make-array (length *footer-marker*) :element-type '(unsigned-byte 8))))
    (read-sequence marker stream)
    (when (equalp marker *footer-marker*) 
      (let ((buffer (make-instance 'rs::serialization-buffer)))
	(rs::load-buffer buffer stream (- *footer-length* (length *footer-marker*)))
	(let ((footer (rs::deserialize buffer))
	      (checksum (rs::deserialize buffer)))
	  (when (equalp checksum (object-checksum footer))
	    footer))))))


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
			  btree)))))))

(defun close-btree (path)
  (bordeaux-threads:with-lock-held (=big-btree-lock=)
    (remhash path *btrees*)))


(defmethod update-btree :around ((btree single-file-btree) &rest args &key (action :insert) &allow-other-keys)
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
		(let ((footer (make-btree-footer 
			       btree (btree-file-footer current-btree) 
			       :action action)))
		  (setf (btree-file-footer-address footer) (file-position *btree-stream*))
		  (write-file-footer *btree-stream* footer)
		  (setf (btree-file-footer btree) footer)
		  (setf (btree-lock btree) lock)
		  (setf (gethash (btree-pathname btree) *btrees*) btree))))))
	  (apply #'update-btree current-btree args))))
      






   