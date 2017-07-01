package io.greennav.persistence;

import org.apache.log4j.Logger;

import java.io.*;
import java.sql.SQLException;
import java.util.Scanner;

/**
 * Created by Hemal on 27-Jun-17.
 */
public class DatabaseApplication
{
	private static Persistence persistence = null;
	private static boolean importScriptLoaded = false;
	private static Logger logger = Logger.getLogger(DatabaseApplication.class.getName());

	public static Persistence getPersistence()
	{
		if(persistence == null) persistence = Persistence.getInstance(logger);
		return persistence;
	}

	public static void main(String[] args)
	{
		Persistence p = getPersistence().databaseName("greennavdb")
				.user("greennav")
				.password("testpassword")
				.queryLimit(20);
		try
		{
			p.connect();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			System.out.println("Connection to database failed. Exiting.");
		}

		Scanner s = new Scanner(System.in);
		while(true)
		{
			System.out.print("> ");
			if(!s.hasNext())
			{
				System.out.println();
				return;
			}
			String command = s.next();
			if(command.equals("import"))
			{
				String fileName = s.next();
				System.out.println(fileName);
				try
				{
					String currentDir = System.getProperty("user.dir");
					logger.info("Checking if import script exists in current directory: " + currentDir);
					if(!importScriptLoaded && !(new File(currentDir + "/importpbf.sh").exists()))
					{
						logger.info("Import script does not exist");

						InputStream importScript = DatabaseApplication.class.getClassLoader().getResourceAsStream("importpbf.sh");
						logger.info("InputStream for import script read from resource opened");
						OutputStream out = new FileOutputStream(new File(currentDir + "/importpbf.sh"));
						logger.info("OutputStream for import script opened");

						int read = 0;
						byte[] bytes = new byte[1024];
						while((read = importScript.read(bytes)) != -1)
						{
							out.write(bytes, 0, read);
						}
						logger.info("Import script written in current directory: " + currentDir);
						importScriptLoaded = true;
					}

					Process importProcess = new ProcessBuilder("bash", currentDir + "/importpbf.sh", fileName).start();
					logger.info("Import script started");
					BufferedReader op = new BufferedReader(new InputStreamReader(importProcess.getInputStream()));
					String line;
					while((line = op.readLine()) != null)
					{
						System.out.println(line);
					}
					importProcess.waitFor();
					logger.info("Import script ended");
				}
				catch (IOException e)
				{
					e.printStackTrace();
					logger.error("Import failed");
					System.out.println("Cannot import");
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
					logger.error("Interrupted while waiting for import script to finish");
					System.out.println("Import failed");
				}
			}
			else if(command.equals("exit"))
			{
				return;
			}
			else
			{
				System.out.println("Invalid command");
				System.out.println("You can use following commands:");
				System.out.println("exit - Exits the interactive mode of application and stops the application");
				System.out.println("help - Opens this help");
				System.out.println("import <filename> - Imports a PBF file to PostGIS enabled PostgreSQL database");
			}
		}
	}
}
