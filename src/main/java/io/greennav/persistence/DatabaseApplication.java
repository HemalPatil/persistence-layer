package io.greennav.persistence;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Scanner;

/**
 * Created by Hemal on 27-Jun-17.
 */
public class DatabaseApplication
{
	private static Persistence persistence = null;
	public static Persistence getPersistence()
	{
		if(persistence == null) persistence = Persistence.getInstance();
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
			String command = s.next();
			if(command.equals("import"))
			{
				System.out.print("Enter file name: ");
				String fileName = s.next();
				System.out.println(System.getProperty("user.dir"));
				System.out.println(fileName);
				try
				{
					Process importScript = new ProcessBuilder("./importpbf", fileName).start();
					BufferedReader op = new BufferedReader(new InputStreamReader(importScript.getInputStream()));
					String line;
					while((line = op.readLine()) != null)
					{
						System.out.println(line);
					}
					importScript.waitFor();
				}
				catch (IOException e)
				{
					e.printStackTrace();
					System.out.println("Cannot import");
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
					System.out.println("Import failed");
				}
			}
		}
	}
}
