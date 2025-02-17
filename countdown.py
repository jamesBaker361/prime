from binary import run_command

big=10000000

while run_command(big):
    big-=10

while run_command(big)==False:
    big+=1

print("first fails when M=",big)