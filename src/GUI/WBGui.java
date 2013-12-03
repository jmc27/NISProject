package GUI;

/**
 * 
 */

import java.awt.Checkbox;
import java.awt.Color;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

/**
 * This creates a window that allow the experimenter to create a session script
 * that will be used to run experiments.
 * 
 * @author Jonathan Chu
 * 
 */

// REFACTOR: this code has not been refactored yet.


public class WBGui extends JFrame {

	JButton scripted;

	JPanel buttonPanel;

	/**
	 * create the Experimenter window with the two buttons
	 */
	public WBGui() {

		super("Search");
		this.setSize(500, 100);
		this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		buttonPanel = new JPanel();
		JTextField search = new JTextField("Text");
		// Button to view script window
		scripted = new JButton("Search");
		scripted.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				
			}
		});

		
		buttonPanel.setLayout(new GridLayout(2, 2));
		buttonPanel.setBorder(javax.swing.BorderFactory
				.createTitledBorder("Web Base Gui"));
		buttonPanel.add(new JLabel("Search for:"));
		buttonPanel.add(search);
		buttonPanel.add(scripted);
		this.add(buttonPanel);
		//this.pack();
	}

}
