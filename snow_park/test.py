# coding:utf-8
import paramiko
sftp_url = '10.86.113.219'
sftp_user = 'zhiye'
sftp_pwd = 'Ab123456'
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(sftp_url,22, sftp_user, sftp_pwd)
sftp = ssh.open_sftp()
files = sftp.listdir()
print(files)
sftp.close()
