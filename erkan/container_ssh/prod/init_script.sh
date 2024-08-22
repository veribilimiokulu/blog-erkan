#!/bin/bash

systemctl start sshd 
sleep 3 
rm -f /run/nologin

# Execute the original CMD
exec ""
