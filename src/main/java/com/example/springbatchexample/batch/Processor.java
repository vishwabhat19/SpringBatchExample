package com.example.springbatchexample.batch;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.example.springbatchexample.model.User;

@Component
public class Processor implements ItemProcessor<User, User> {

	private static final Map<String,String> DEPT_NAMES = 
			new HashMap<>();
	
	public Processor(){
	DEPT_NAMES.put("1","Technology");
	DEPT_NAMES.put("2","Operations");
	DEPT_NAMES.put("3","Accounts");
	
	}
	@Override
	public User process(User user) throws Exception {
		
		String deptCode = user.getDept();
		String dept = DEPT_NAMES.get(deptCode);
		user.setDept(dept);
		System.out.println(String.format("Converted from [%s] to [%s]", deptCode,dept));
		return user;
	}

	
}
