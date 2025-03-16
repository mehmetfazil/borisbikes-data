job "tfl_to_db" {
  datacenters = ["dc1"]
  type = "batch"

  periodic {
    crons = ["*/10 * * * * * *"]  # Every 10 seconds
  }

  group "borisbikes" {
    task "tfl_to_db" {
      driver = "raw_exec"
      config {
        command = "/root/borisbikes-data/.venv/bin/python"
        args    = ["/root/borisbikes-data/tfl_to_db.py"]
	work_dir = "/root/borisbikes-data"
      }
      
   }
  }
}
