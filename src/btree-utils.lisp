(in-package :planks.btree)

 ;;
 ;; Comparison functions that can be deduced from KEY< (because the
 ;; btree keys have a total order).
 ;;

 (defmethod btree-key< ((btree btree))
   (let ((key< (slot-value btree 'key<))
	 (key-key (btree-key-key btree)))
     (lambda (key1 key2)
       (and 
	    (funcall key<
		     (funcall key-key key1)
		     (funcall key-key key2))))))

 (defmethod btree-key= ((btree btree))
   (let ((key< (slot-value btree 'key<))
	 (key-key (btree-key-key btree)))
     (lambda (key1 key2)
       (let ((key1 (funcall key-key key1))
		  (key2 (funcall key-key key2)))
	      (and (not (funcall key< key1 key2))
		   (not (funcall key< key2 key1)))))))

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

;;; counts, depth, balanace
(defun btree-node-binding-count (node)
   (length (btree-node-index node)))

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

