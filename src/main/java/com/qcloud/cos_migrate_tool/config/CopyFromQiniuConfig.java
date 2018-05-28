package com.qcloud.cos_migrate_tool.config;


public class CopyFromQiniuConfig extends CopyFromCompetitorConfig {
	private boolean isNeedSign = true;
	
	public boolean IsNeedSign() {
		return isNeedSign;
	}
	
	public void setIsNeedSign(boolean isNeedSign) {
		this.isNeedSign = isNeedSign;
	}
	
	
}
