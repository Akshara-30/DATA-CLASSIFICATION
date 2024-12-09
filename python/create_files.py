count = 0;
max = 100000

while True:
    file_name = f'file_{count}.txt' 
    print(f'Creating file: {file_name}')
    with open(file_name, "w+") as file:
        file.write("Hello, World!");
    count += 1; 

    if count > max:
        break;