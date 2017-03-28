===========================================
HDFS Contents Manager for Jupyter Notebooks
===========================================

A contents manager for Jupyter that stores files in Hadoop File System (HDFS)


Install
-------

install HDFS3_.

Clone this repository then pip install .


Run
----

Example:
jupyter-notebook --NotebookApp.contents_manager_class='hdfscontents.hdfsmanager.HDFSContentsManager' --HDFSContentsManager.hdfs_namenode_host='localhost' --HDFSContentsManager.hdfs_namenode_port=9000 --HDFSContentsManager.root_dir='/user/centos/'

Alternatively run jupyter notebook --generate-config and add the above configs in the generated config file


.. _HDFS3: https://hdfs3.readthedocs.io/en/latest/install.html
