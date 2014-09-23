# This is an example of how the SWF daemons might be started/reloaded
# in a project deployed with Capistrano 2. If you are not using Capistrano,
# see :restart below for the general strategy that you can implement in your
# own deployment solution.

set :application, "my_app"


namespace :swf do

  task :start_workers, roles do
    run "#{deploy_to}/current/scripts/swf_worker_server.sh start"
  end

  task :start_deciders do
    run "#{deploy_to}/current/scripts/swf_decider_server.sh start"
  end

  task :stop_workers do
    run "#{deploy_to}/current/scripts/swf_worker_server.sh stop"
  end

  task :stop_deciders do
    run "#{deploy_to}/current/scripts/swf_decider_server.sh stop"
  end

  task :monitor do
    run "monit"
    sleep(1)
    run "monit monitor -g swfworkers"
    run "monit monitor -g swfdeciders"
  end

  task :unmonitor do
    run "monit"
    sleep(1)
    run "monit unmonitor -g swfworkers"
    run "monit unmonitor -g swfdeciders"
  end

  task :start do
    start_deciders
    start_workers
    monitor
  end

  task :stop do
    unmonitor
    stop_deciders
    stop_workers
  end

  task :restart do
    unmonitor
    stop_deciders
    stop_workers
    puts 'Waiting 60 seconds for long-polling SWF daemons to quit'
    sleep(61)
    start_deciders
    start_workers
    monitor
  end

  after "deploy:restart", "swf:restart"

end
