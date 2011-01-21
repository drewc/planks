
(asdf:defsystem #:planks 
  :author "Drew Crampsie <drewc@tech.coop>"
  :licence "MIT"
  :components ((:module :src
		:components ((:file "btree-protocol")
			     (:file "btree")
			     (:file "map-btree")
			     (:file "file-btree")
			     (:file "view")
			     (:file "btree-class")
			     (:file "object-btree")
			     (:file "persistent-objects"))		
		:serial t))

  :depends-on (:rucksack
	       :ironclad
	       :bordeaux-threads
	       :babel
	       :closer-mop))


		