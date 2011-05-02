(in-package :cl)

(defpackage :planks.btree-test 
  (:use :cl :planks.btree))

(in-package :planks.btree-test)

(defvar *path* "/tmp/btree")



(defun test-file-btree-insert (&key (path *path*))
  (let ((bt (make-btree path :if-exists :supersede)))
    (assert (null (btree-search bt 1 :errorp nil)))
    (assert (eq 'error (handler-case (btree-search bt 1)
			 (btree-search-error () 'error))))
    (let ((bt2 (btree-insert bt 1 "one")))
      (assert (null (btree-search bt 1 :errorp nil)))
      (assert (equal "one" (btree-search bt2 1)))))

  (close-btree path)

  (let ((bt (find-btree path)))
    (assert (equal "one" (btree-search bt 1)))))

(defun test-btree-balance (&key (path *path*))
  (let ((bt (make-btree path :if-exists :supersede :max-node-size 5)))
    (assert (null (btree-search bt 1 :errorp nil)))
    (loop for i upto 1234 for b = (btree-insert bt i i) then (btree-insert b i i) 
       :finally (assert (planks.btree::btree-balanced-p b)))))

(defun reverse-map (k v)
  (list (cons v k)))

(defun add-one-to-key (k v)
  (declare (ignore v))
  (list (cons k (1+ k))))

(defun length-of-value (k v)
  (declare (ignore k))
  (list (cons v (length v))))

(defun test-multi-btree-existing-data-insert (&key (path *path*))
  (let ((bt (make-btree path 
			:if-exists :supersede
			:class 'multi-btree))
	(data (remove-duplicates 
	       (loop 
		  for i upto 1234 
		  for r = (format nil "~R" i)
		  :collect (cons i r))
	       :key #'car)))
    (loop  
       for (i . r) in data	 
       for b = (btree-insert bt i r) 
       then (btree-insert b i r))

    (add-function-btree (find-btree path) 'reverse-map :key= 'equal :key< 'string<)

    (add-function-btree (find-btree path) 'add-one-to-key)
    (add-function-btree (find-btree path) 'length-of-value :key= 'equal :key< 'string<)
    (find-btree path)))

(defun test-multi-btree-existing-data (&optional (bt (test-multi-btree-existing-data-insert)))
  
  (map-btree (find-function-btree bt 'reverse-map) 
	     (lambda (k v)
	       (assert (equal k (btree-search bt v)))))

  (map-btree (find-function-btree bt 'add-one-to-key) 
	     (lambda (k v)
	       (assert (equal (1+ k) v))))

  (map-btree (find-function-btree bt 'length-of-value) 
	     (lambda (k v)
	       (assert (equal (length k) v)))))

(defun test-heap-btree ()
  (make-btree *path*
	      :if-exists :supersede
	      :max-node-size 10
	      :class 'heap-btree)
  (symbol-macrolet ((bt (find-btree *path*)))
    (loop for n to 115000 do  (btree-insert bt n n))
    (format t "File Size : ~A" (float (/ (btree-file-size bt) (* 1024 1024))))
    bt))

(defun test-heap-btree-more ()
  (make-btree *path*
	      :if-exists :supersede
	      :max-node-size 10
	      :class 'heap-btree
	      :key< 'string<
	      :key= 'equalp)
  (symbol-macrolet ((bt (find-btree *path*)))
    (loop for n to 115000 do  
	 (btree-insert bt (uuid:print-bytes nil (uuid:make-v1-uuid)) n)
	 (close-btree bt))
	 
    (format t "File Size : ~A" (float (/ (btree-file-size bt) (* 1024 1024))))
    bt))
    
  
	       
(progn 
  (test-file-btree-insert)
  (test-btree-balance)
  (test-multi-btree-existing-data))
    



       
  

